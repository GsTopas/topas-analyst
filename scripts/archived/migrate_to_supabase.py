"""
One-shot migration: dump alle data fra lokale snapshots.db + catalog.db
ind i Supabase Postgres.

Bruger psycopg2 til direkte Postgres-forbindelse (ikke Supabase REST API),
så script'et kan køre lokalt fra din PC mod Supabase i skyen.

Forudsætninger:
  1. Tabellerne findes i Supabase (allerede oprettet via apply_migration)
  2. SUPABASE_DB_URL er sat i .env — Connection String fra Supabase dashboard
     (Settings → Database → Connection String → URI). Den ser typisk ud sådan:
       postgresql://postgres.bymurhqfcyxdhrayddoz:[PASSWORD]@aws-0-eu-north-1.pooler.supabase.com:6543/postgres
  3. `pip install psycopg2-binary` (tilføjet til requirements.txt)

Usage:
  cd C:\\Users\\gs\\Downloads\\topas-scraper
  pip install psycopg2-binary
  python migrate_to_supabase.py            # dry-run, viser hvad der vil ske
  python migrate_to_supabase.py --apply    # gennemfør migration

Idempotent: kører `TRUNCATE` på alle target-tabeller før insert,
så scriptet kan køres flere gange uden duplikater.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Iterable, Optional

from dotenv import load_dotenv

load_dotenv()

try:
    import psycopg2
    from psycopg2.extras import execute_batch
except ImportError:
    print("FEJL: psycopg2 ikke installeret.")
    print("Kør: pip install psycopg2-binary")
    sys.exit(1)


SNAPSHOTS_DB = Path("data/snapshots.db")
CATALOG_DB = Path("data/catalog.db")

# Tabeller fra hver SQLite-fil. Rækkefølge matters pga. foreign keys —
# parent-tabeller først, child-tabeller efter.
SNAPSHOTS_TABLES = [
    "scraper_runs",
    "tours",
    "departures",
    "snapshots",
    "topas_catalog",
]
CATALOG_TABLES = [
    "operators",
    "catalog_tours",
    "tour_extractions",
    "tour_classifications",
    "match_proposals",
    "review_decisions",
    "n8n_candidates",
    "approved_competitor_targets",
    "pattern_observations",
]

# Felter der i SQLite er INTEGER (0/1) men i Postgres er BOOLEAN.
# Disse bliver konverteret on-the-fly under migration.
BOOLEAN_FIELDS = {
    ("tours", "fællesrejse_eligible"),
}

# Truncate-rækkefølge: omvendt af insert-rækkefølge for at respektere FK-constraints
# (slet children først, så parents)
TRUNCATE_ORDER = list(reversed(CATALOG_TABLES + SNAPSHOTS_TABLES))


def get_dsn() -> str:
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        print("FEJL: SUPABASE_DB_URL er ikke sat i .env")
        print()
        print("Hent forbindelses-strengen fra Supabase:")
        print("  1. Gå til https://supabase.com/dashboard/project/bymurhqfcyxdhrayddoz/settings/database")
        print("  2. Under 'Connection string' → vælg 'URI' tab")
        print("  3. Kopiér strengen (vælg 'Transaction pooler' for kort-livede forbindelser)")
        print("  4. Erstat [YOUR-PASSWORD] med din DB-password")
        print("  5. Tilføj til .env: SUPABASE_DB_URL=postgresql://...")
        sys.exit(1)
    return dsn


def fetch_rows(sqlite_path: Path, table: str) -> tuple[list[str], list[tuple]]:
    """Hent kolonnenavne + rækker fra en SQLite-tabel."""
    if not sqlite_path.exists():
        print(f"  SQLite fil mangler: {sqlite_path}")
        return [], []
    conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        # Kolonner fra schema
        cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if not cols:
            return [], []
        # Rækker
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        return cols, [tuple(r) for r in rows]
    finally:
        conn.close()


def coerce_row(table: str, cols: list[str], row: tuple) -> tuple:
    """Konverter SQLite 0/1-værdier til Postgres BOOLEAN hvor relevant."""
    out = list(row)
    for i, c in enumerate(cols):
        if (table, c) in BOOLEAN_FIELDS and out[i] is not None:
            # SQLite gemmer som 0/1; Postgres vil have True/False
            out[i] = bool(out[i])
    return tuple(out)


def migrate_table(
    pg_cur,
    sqlite_path: Path,
    table: str,
    apply: bool,
) -> None:
    cols, rows = fetch_rows(sqlite_path, table)
    if not cols:
        print(f"  {table}: tabel ikke fundet eller tom — sprunget over")
        return
    if not rows:
        print(f"  {table}: 0 rækker")
        return

    print(f"  {table}: {len(rows)} rækker", end="")
    if not apply:
        print("  (dry-run)")
        return

    # Bygger INSERT-statement: INSERT INTO {table} ({cols}) VALUES (%s, %s, ...)
    # Bruger eksplicit kolonneliste for at være robust mod kolonne-rækkefølge
    placeholders = ", ".join(["%s"] * len(cols))
    quoted_cols = ", ".join(f'"{c}"' for c in cols)
    sql = f'INSERT INTO public.{table} ({quoted_cols}) VALUES ({placeholders})'

    coerced = [coerce_row(table, cols, r) for r in rows]
    execute_batch(pg_cur, sql, coerced, page_size=500)
    print(" → indlæst")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Gennemfør migration. Uden flag = dry-run.")
    args = parser.parse_args()

    if not SNAPSHOTS_DB.exists() and not CATALOG_DB.exists():
        print(f"FEJL: ingen lokale SQLite-filer fundet ({SNAPSHOTS_DB}, {CATALOG_DB})")
        sys.exit(1)

    dsn = get_dsn()
    print(f"Forbinder til Supabase ...")
    print(f"  DSN: {dsn.split('@')[1] if '@' in dsn else '(skjult)'}")
    print()

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        if args.apply:
            print("Truncating target-tabeller (omvendt FK-rækkefølge) ...")
            for t in TRUNCATE_ORDER:
                cur.execute(f"TRUNCATE TABLE public.{t} RESTART IDENTITY CASCADE")
            print(f"  {len(TRUNCATE_ORDER)} tabeller truncated\n")

        print("=" * 60)
        print("snapshots.db → Supabase")
        print("=" * 60)
        for t in SNAPSHOTS_TABLES:
            migrate_table(cur, SNAPSHOTS_DB, t, args.apply)

        print()
        print("=" * 60)
        print("catalog.db → Supabase")
        print("=" * 60)
        for t in CATALOG_TABLES:
            migrate_table(cur, CATALOG_DB, t, args.apply)

        if args.apply:
            # Sync BIGSERIAL-sequences til MAX(id) — ellers fejler INSERTs uden
            # eksplicit id med UniqueViolation fordi sequence-counter starter ved 1.
            print("\nSync'er BIGSERIAL-sequences til MAX(id)...")
            sequence_fixes = [
                ("snapshots", "snapshot_id"),
                ("tour_classifications", "id"),
                ("match_proposals", "id"),
                ("review_decisions", "id"),
                ("approved_competitor_targets", "id"),
                ("pattern_observations", "id"),
            ]
            for table, col in sequence_fixes:
                cur.execute(
                    f"SELECT setval(pg_get_serial_sequence('{table}', '{col}'), "
                    f"COALESCE((SELECT MAX({col}) FROM {table}), 1), true)"
                )
            conn.commit()
            print(f"  {len(sequence_fixes)} sequences synced")
            print("\n✓ Færdig — alle data committed til Supabase.")
        else:
            print("\nDry-run færdig. Tilføj --apply for at skrive til Supabase.")
    except Exception as e:
        conn.rollback()
        print(f"\nFEJL: {e}")
        print("Rollback foretaget — Supabase-data er uændret.")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
