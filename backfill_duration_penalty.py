"""
Backfill: anvend duration-penalty på allerede importerede n8n_candidates.

Samme regel som v22.2 i n8n:
    tolerance = max(2 dage, 15% af Topas-varigheden)
    udenfor [topas-tol, topas+tol] → downgrade ét niveau (high→medium, medium→low)

Topas-varigheden hentes via topas_tour_code → topas_catalog.duration_days.
Hvis Topas-turen mangler duration_days, springer scriptet over de kandidater
(de kan ikke evalueres deterministisk).

Idempotent: rækker der allerede har "[Auto-downgrade" i notes preskippes.

Usage:
    cd C:\\Users\\gs\\Downloads\\topas-scraper
    python backfill_duration_penalty.py            # dry-run, viser hvad der vil ske
    python backfill_duration_penalty.py --apply    # gennemfør
"""
from __future__ import annotations

import argparse
import math
import sqlite3
from pathlib import Path

CATALOG_DB = Path("data/catalog.db")
SNAPSHOTS_DB = Path("data/snapshots.db")

DOWNGRADE = {"high": "medium", "medium": "low", "low": "low"}


def get_topas_durations(snapshots_path: Path) -> dict[str, int]:
    """tour_code -> duration_days fra topas_catalog (kun rækker med varighed)."""
    conn = sqlite3.connect(snapshots_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT tour_code, duration_days FROM topas_catalog "
        "WHERE tour_code IS NOT NULL AND duration_days IS NOT NULL "
        "  AND duration_days > 0"
    ).fetchall()
    conn.close()
    return {r["tour_code"]: int(r["duration_days"]) for r in rows}


def evaluate(match_conf: str, comp_dur, topas_dur):
    """Returnerer (new_confidence, applied, lower, upper) eller None hvis ikke evaluerbar."""
    if not match_conf or comp_dur is None or topas_dur is None:
        return None
    try:
        comp_dur = int(comp_dur)
        topas_dur = int(topas_dur)
    except (TypeError, ValueError):
        return None
    if comp_dur <= 0 or topas_dur <= 0:
        return None

    tolerance = max(2.0, topas_dur * 0.15)
    lower = topas_dur - tolerance
    upper = topas_dur + tolerance
    int_lower = math.ceil(lower)
    int_upper = math.floor(upper)

    if comp_dur < lower or comp_dur > upper:
        new_c = DOWNGRADE.get(match_conf, match_conf)
        return new_c, (new_c != match_conf), int_lower, int_upper
    return match_conf, False, int_lower, int_upper


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Skriv ændringer til DB. Uden flag = dry-run.",
    )
    args = parser.parse_args()

    if not CATALOG_DB.exists():
        print(f"Mangler {CATALOG_DB.resolve()}")
        return
    if not SNAPSHOTS_DB.exists():
        print(f"Mangler {SNAPSHOTS_DB.resolve()} — kan ikke hente Topas-varigheder")
        return

    topas_durations = get_topas_durations(SNAPSHOTS_DB)
    print(f"Topas-katalog: {len(topas_durations)} ture med varighed")

    conn = sqlite3.connect(CATALOG_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT n8n_row_id, topas_tour_code, duration_days, match_confidence, notes
        FROM n8n_candidates
        WHERE topas_tour_code IS NOT NULL
          AND match_confidence IN ('high', 'medium')
        """
    ).fetchall()
    print(f"Kandidater at gennemgå (high/medium): {len(rows)}\n")

    updates: list[tuple] = []
    skipped_no_topas = 0
    skipped_already = 0
    skipped_no_comp_dur = 0
    in_range = 0

    for r in rows:
        topas_dur = topas_durations.get(r["topas_tour_code"])
        if topas_dur is None:
            skipped_no_topas += 1
            continue
        if r["duration_days"] is None:
            skipped_no_comp_dur += 1
            continue
        if "[Auto-downgrade" in (r["notes"] or ""):
            skipped_already += 1
            continue

        result = evaluate(r["match_confidence"], r["duration_days"], topas_dur)
        if result is None:
            continue
        new_c, applied, lo, hi = result
        if not applied:
            in_range += 1
            continue

        new_notes = (r["notes"] or "").rstrip() + (
            f" [Auto-downgrade-backfill: {r['duration_days']}d vs Topas "
            f"{topas_dur}d (range {lo}-{hi})]"
        ).strip()
        updates.append(
            (
                r["n8n_row_id"],
                r["topas_tour_code"],
                r["match_confidence"],
                new_c,
                r["duration_days"],
                topas_dur,
                new_notes,
            )
        )

    print(f"Sprunget over (Topas mangler varighed):  {skipped_no_topas}")
    print(f"Sprunget over (kandidat mangler varighed): {skipped_no_comp_dur}")
    print(f"Sprunget over (allerede backfilled):       {skipped_already}")
    print(f"Inden for tolerance (uændret):             {in_range}")
    print(f"Til downgrade:                             {len(updates)}\n")

    if not updates:
        print("Intet at gøre.")
        conn.close()
        return

    print("Eksempler:")
    for u in updates[:25]:
        print(f"  #{u[0]:>4} {u[1]:<8} {u[2]:>6} → {u[3]:>6}   ({u[4]}d vs Topas {u[5]}d)")
    if len(updates) > 25:
        print(f"  ... og {len(updates) - 25} flere")

    if not args.apply:
        print("\nDry-run. Tilføj --apply for at skrive ændringerne.")
        conn.close()
        return

    print(f"\nSkriver {len(updates)} ændringer...")
    cur = conn.cursor()
    for n8n_id, _code, _old, new_c, _cd, _td, new_notes in updates:
        cur.execute(
            "UPDATE n8n_candidates SET match_confidence=?, notes=? WHERE n8n_row_id=?",
            (new_c, new_notes, n8n_id),
        )
    conn.commit()
    conn.close()
    print(f"Færdig. {len(updates)} rækker opdateret.")


if __name__ == "__main__":
    main()
