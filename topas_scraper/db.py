"""
SQLite schema and helpers.

Implements the data model from `taxonomy.md` §2.9:
- tours        — one row per tour (Topas + competitor)
- departures   — current per-departure prices (overwritten each scraper run)
- snapshots    — append-only history of price/status per departure per run

Why both `departures` (current) and `snapshots` (history)?
  - departures: easy lookup of "what's the current price for tour X afgang Y"
  - snapshots:  enables week-over-week diffs and price-volatility flags

scraper_run_id ties everything together. Each invocation of `scrape` gets a UUID;
the diff command compares run N against run N-1.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from uuid import uuid4

from psycopg2.extras import execute_values

from .config import DEFAULT_DB_PATH
from . import _pg_conn

# Type-alias for læsbarhed — connect() returnerer en PgConnection-wrapper
# der efterligner sqlite3.Connection's API.
Connection = _pg_conn.PgConnection


# Skemaet ligger i Supabase (kørt via apply_migration én gang). SCHEMA_SQL
# bevares som reference / dokumentation, men eksekveres ikke længere på
# connect — Supabase er sandheden.
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS scraper_runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TEXT NOT NULL,    -- ISO 8601 UTC
    finished_at     TEXT,
    target_count    INTEGER NOT NULL,
    success_count   INTEGER NOT NULL DEFAULT 0,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS tours (
    -- Identity
    operator        TEXT NOT NULL,
    tour_code       TEXT,             -- Topas turkode if Topas; else NULL
    tour_slug       TEXT NOT NULL,    -- normalized identifier across operators
    tour_name       TEXT NOT NULL,
    url             TEXT NOT NULL,
    -- Geography
    country         TEXT NOT NULL,
    region          TEXT,
    -- Relationship: which Topas tour-code this tour competes against
    -- (Topas's own row has competes_with = its own tour_code, e.g. "PTMD")
    competes_with   TEXT,
    -- Structure
    tour_format     TEXT,             -- Fællesrejse / Individuel / Privat gruppe
    duration_days   INTEGER,
    -- Pricing summary
    from_price_dkk  INTEGER,          -- headline "fra X kr." for display only
    -- Eligibility
    fællesrejse_eligible BOOLEAN,
    eligibility_notes    TEXT,
    -- Tour-level meal info (extracted via topas_scraper.meals)
    meals_included       INTEGER,         -- best-estimate count, NULL if unknown
    meals_description    TEXT,             -- short Danish summary
    -- Metadata
    last_seen_run   TEXT NOT NULL,    -- FK to scraper_runs.run_id
    PRIMARY KEY (operator, tour_slug)
);

CREATE TABLE IF NOT EXISTS departures (
    operator        TEXT NOT NULL,
    tour_slug       TEXT NOT NULL,
    departure_code  TEXT,             -- Topas tripCode if available; else generated
    start_date      TEXT NOT NULL,    -- ISO 8601 date
    end_date        TEXT,
    price_dkk       INTEGER,          -- NULL when "Afventer pris"
    availability_status TEXT,
    flight_origin   TEXT,             -- København, Aalborg, etc.
    rejseleder_name TEXT,
    last_seen_run   TEXT NOT NULL,
    PRIMARY KEY (operator, tour_slug, start_date),
    FOREIGN KEY (operator, tour_slug) REFERENCES tours(operator, tour_slug)
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL,
    operator        TEXT NOT NULL,
    tour_slug       TEXT NOT NULL,
    start_date      TEXT NOT NULL,
    price_dkk       INTEGER,
    availability_status TEXT,
    observed_at     TEXT NOT NULL,    -- ISO 8601 UTC
    FOREIGN KEY (run_id) REFERENCES scraper_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_departures_run ON departures(last_seen_run);
CREATE INDEX IF NOT EXISTS idx_snapshots_run ON snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_tour ON snapshots(operator, tour_slug, start_date);

-- topas_catalog: the authoritative list of Topas Fællesrejse-med-turleder products.
-- Refreshed monthly (or on demand) from topas.dk's filtered search page. Separate
-- from `tours` because catalog tracks "what Topas sells" while tours tracks
-- "what we've scraped pricing for". A Topas tour can be in catalog but not yet
-- have competitor mapping in TARGETS.
CREATE TABLE IF NOT EXISTS topas_catalog (
    tour_code           TEXT,             -- e.g. "VNSN" — may be NULL until product page is scraped
    tour_name           TEXT NOT NULL,
    url                 TEXT PRIMARY KEY, -- canonical Topas product URL
    country             TEXT,
    duration_days       INTEGER,
    from_price_dkk      INTEGER,
    audience_segment    TEXT,             -- 'Åben', '30-50 år', etc.
    discovered_at       TEXT NOT NULL,    -- ISO 8601 UTC, when first seen
    last_seen_at        TEXT NOT NULL,    -- ISO 8601 UTC, last time URL was in catalog
    has_competitor_mapping INTEGER NOT NULL DEFAULT 0  -- 1 if tour_code is in TARGETS competes_with set
);

CREATE INDEX IF NOT EXISTS idx_topas_catalog_code ON topas_catalog(tour_code);
CREATE INDEX IF NOT EXISTS idx_topas_catalog_country ON topas_catalog(country);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: Optional[Path] = None) -> Connection:
    """Åbn forbindelse til Supabase Postgres. db_path-argumentet ignoreres
    (bevaret for kompatibilitet med callers der historisk gav SQLite-sti)."""
    return _pg_conn.connect()


# _migrate_schema fjernet — alle migrations kører nu via Supabase
# apply_migration ad hoc. Hvis skemaet skal ændres, gøres det centralt i
# Supabase (én gang) i stedet for at hver app-instans selv migrerer.


def start_run(conn: Connection, target_count: int) -> str:
    """Open a new scraper_run row and return the run_id."""
    run_id = str(uuid4())
    conn.execute(
        "INSERT INTO scraper_runs (run_id, started_at, target_count) VALUES (?, ?, ?)",
        (run_id, _now_iso(), target_count),
    )
    conn.commit()
    return run_id


def finish_run(conn: Connection, run_id: str, success_count: int, notes: str = "") -> None:
    conn.execute(
        "UPDATE scraper_runs SET finished_at = ?, success_count = ?, notes = ? WHERE run_id = ?",
        (_now_iso(), success_count, notes, run_id),
    )
    conn.commit()


def upsert_tour(conn: Connection, tour: dict, run_id: str) -> None:
    """Insert or replace a tour row keyed by (operator, tour_slug)."""
    conn.execute(
        """
        INSERT INTO tours (
            operator, tour_code, tour_slug, tour_name, url,
            country, region, competes_with, tour_format, duration_days,
            from_price_dkk, fællesrejse_eligible, eligibility_notes,
            meals_included, meals_description, last_seen_run
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(operator, tour_slug) DO UPDATE SET
            tour_code = excluded.tour_code,
            tour_name = excluded.tour_name,
            url = excluded.url,
            country = excluded.country,
            region = excluded.region,
            competes_with = excluded.competes_with,
            tour_format = excluded.tour_format,
            duration_days = excluded.duration_days,
            from_price_dkk = excluded.from_price_dkk,
            fællesrejse_eligible = excluded.fællesrejse_eligible,
            eligibility_notes = excluded.eligibility_notes,
            meals_included = excluded.meals_included,
            meals_description = excluded.meals_description,
            last_seen_run = excluded.last_seen_run
        """,
        (
            tour["operator"], tour.get("tour_code"), tour["tour_slug"], tour["tour_name"], tour["url"],
            tour.get("country"), tour.get("region"), tour.get("competes_with"),
            tour.get("tour_format"), tour.get("duration_days"),
            tour.get("from_price_dkk"), tour.get("fællesrejse_eligible"),
            tour.get("eligibility_notes"),
            tour.get("meals_included"), tour.get("meals_description"),
            run_id,
        ),
    )
    conn.commit()


def replace_departures(
    conn: Connection,
    operator: str,
    tour_slug: str,
    departures: Iterable[dict],
    run_id: str,
) -> int:
    """Replace all departures for this tour and append snapshots.

    Bruger execute_values (psycopg2.extras) til batch-insert af både
    departures og snapshots i én roundtrip hver. Tidligere kørte koden
    2N individuelle INSERTs per scrape gennem PgBouncer Transaction Pooler;
    den åbne transaktion var derudover sårbar over for idle-in-transaction
    timeouts.
    """
    deps = list(departures)
    now = _now_iso()

    raw_cur = conn._conn.cursor()
    try:
        # Wipe existing departure rows for this tour — current state only.
        raw_cur.execute(
            "DELETE FROM departures WHERE operator = %s AND tour_slug = %s",
            (operator, tour_slug),
        )

        if deps:
            dep_rows = [
                (
                    operator, tour_slug, dep.get("departure_code"),
                    dep["start_date"], dep.get("end_date"),
                    dep.get("price_dkk"), dep.get("availability_status"),
                    dep.get("flight_origin"), dep.get("rejseleder_name"), run_id,
                )
                for dep in deps
            ]
            execute_values(
                raw_cur,
                """
                INSERT INTO departures (
                    operator, tour_slug, departure_code, start_date, end_date,
                    price_dkk, availability_status, flight_origin, rejseleder_name, last_seen_run
                ) VALUES %s
                """,
                dep_rows,
            )
            snap_rows = [
                (
                    run_id, operator, tour_slug, dep["start_date"],
                    dep.get("price_dkk"), dep.get("availability_status"), now,
                )
                for dep in deps
            ]
            execute_values(
                raw_cur,
                """
                INSERT INTO snapshots (run_id, operator, tour_slug, start_date, price_dkk, availability_status, observed_at)
                VALUES %s
                """,
                snap_rows,
            )
    finally:
        raw_cur.close()

    conn.commit()
    return len(deps)


def latest_run_id(conn: Connection) -> Optional[str]:
    row = conn.execute("SELECT run_id FROM scraper_runs ORDER BY started_at DESC LIMIT 1").fetchone()
    return row["run_id"] if row else None


def previous_run_id(conn: Connection) -> Optional[str]:
    row = conn.execute("SELECT run_id FROM scraper_runs ORDER BY started_at DESC LIMIT 1 OFFSET 1").fetchone()
    return row["run_id"] if row else None


def fetch_tours(conn: Connection, run_id: Optional[str] = None) -> list[dict]:
    if run_id:
        return conn.execute("SELECT * FROM tours WHERE last_seen_run = ?", (run_id,)).fetchall()
    return conn.execute("SELECT * FROM tours").fetchall()


def fetch_departures(conn: Connection, operator: str, tour_slug: str) -> list[dict]:
    return conn.execute(
        "SELECT * FROM departures WHERE operator = ? AND tour_slug = ? ORDER BY start_date",
        (operator, tour_slug),
    ).fetchall()


def detect_status_anomaly(
    conn: Connection,
    operator: str,
    tour_slug: str,
    start_date: str,
) -> Optional[dict]:
    """Find 'interessante' status-overgange i en departure's historik.

    Returnerer dict hvis anomali fundet, None ellers.

    Anomalies:
      - 'withdrawn': salgsklar (Åben/Garanteret/Få pladser) → 'På forespørgsel'
        — operatør har trukket afgangen fra salg
      - 'fast_sellout': Åben → Udsolgt uden 'Få pladser'-mellemtrin
        — meget hurtigt udsolgt, stærkt efterspørgsels-signal

    'vanished'-detection (afgang fjernet helt fra siden) håndteres separat
    i export.py fordi den kræver tour-level run-context.
    """
    rows = conn.execute(
        """
        SELECT availability_status, price_dkk, observed_at, run_id
        FROM snapshots
        WHERE operator=? AND tour_slug=? AND start_date=?
        ORDER BY observed_at DESC
        """,
        (operator, tour_slug, start_date),
    ).fetchall()

    if len(rows) < 2:
        return None

    def _categorize(s: str | None) -> str:
        s_l = (s or "").strip().lower()
        if s_l in ("åben", "ledig"):
            return "selling"
        if s_l == "garanteret":
            return "selling"
        if s_l in ("få pladser",):
            return "late_selling"
        if s_l in ("på forespørgsel", "afventer pris"):
            return "withdrawn"
        if s_l in ("udsolgt",):
            return "sold_out"
        return "unknown"

    latest = rows[0]
    latest_cat = _categorize(latest["availability_status"])

    # Find seneste tidligere observation med ANDEN kategori
    previous = None
    for r in rows[1:]:
        if _categorize(r["availability_status"]) != latest_cat:
            previous = r
            break

    if previous is None:
        return None

    prev_cat = _categorize(previous["availability_status"])

    base = {
        "previous_state": previous["availability_status"],
        "previous_observed_at": previous["observed_at"],
        "previous_price_dkk": previous["price_dkk"],
        "current_state": latest["availability_status"],
        "current_observed_at": latest["observed_at"],
        "current_price_dkk": latest["price_dkk"],
    }

    # Anomali 1: WITHDRAWN — was selling/late_selling, now withdrawn
    if prev_cat in ("selling", "late_selling") and latest_cat == "withdrawn":
        return {
            "anomaly_type": "withdrawn",
            "severity": "high",
            "label": f"Trukket fra salg (var '{previous['availability_status']}')",
            **base,
        }

    # Anomali 2: FAST_SELLOUT — was selling (Åben), now sold_out (jumped late_selling)
    if prev_cat == "selling" and latest_cat == "sold_out":
        return {
            "anomaly_type": "fast_sellout",
            "severity": "medium",
            "label": "Skiftet til Udsolgt",
            **base,
        }

    return None


def get_price_change(
    conn: Connection,
    operator: str,
    tour_slug: str,
    start_date: str,
    lookback_days: int = 7,
) -> Optional[dict]:
    """Find pris-ændring for én departure siden en observation der er mindst
    `lookback_days` ældre end den seneste.

    Returnerer dict med keys: delta, previous_price, previous_observed_at,
    days_ago, current_price, current_observed_at.
    Returnerer None hvis der ikke findes en kvalificerende prior observation.

    Logik: vi finder den nyeste observation, så finder vi den seneste tidligere
    observation der er ≥ lookback_days ældre. Vi sammenligner deres priser.
    Hvis tour først blev set for nylig (< lookback_days siden), returneres None.
    """
    from datetime import datetime, timedelta  # noqa: PLC0415

    rows = conn.execute(
        """
        SELECT price_dkk, observed_at
        FROM snapshots
        WHERE operator=? AND tour_slug=? AND start_date=?
          AND price_dkk IS NOT NULL
        ORDER BY observed_at DESC
        """,
        (operator, tour_slug, start_date),
    ).fetchall()

    if len(rows) < 2:
        return None

    latest = rows[0]
    latest_price = latest["price_dkk"]
    latest_observed = latest["observed_at"]

    try:
        latest_dt = datetime.fromisoformat(latest_observed.replace("Z", "+00:00").replace("+00:00", ""))
    except (ValueError, AttributeError):
        return None

    cutoff = latest_dt - timedelta(days=lookback_days)

    # Find rows der er ældre end cutoff
    previous = None
    for r in rows[1:]:
        if not r["observed_at"]:
            continue
        try:
            r_dt = datetime.fromisoformat(r["observed_at"].replace("Z", "+00:00").replace("+00:00", ""))
        except ValueError:
            continue
        if r_dt <= cutoff:
            previous = r
            break

    if previous is None:
        return None

    delta = latest_price - previous["price_dkk"]
    try:
        prev_dt = datetime.fromisoformat(previous["observed_at"].replace("Z", "+00:00").replace("+00:00", ""))
        days_ago = (latest_dt - prev_dt).days
    except ValueError:
        days_ago = None

    return {
        "delta": delta,
        "previous_price": previous["price_dkk"],
        "previous_observed_at": previous["observed_at"],
        "days_ago": days_ago,
        "current_price": latest_price,
        "current_observed_at": latest_observed,
    }


def fetch_snapshot_for_run(conn: Connection, run_id: str) -> list[dict]:
    return conn.execute(
        "SELECT * FROM snapshots WHERE run_id = ? ORDER BY operator, tour_slug, start_date",
        (run_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Topas catalog
# ---------------------------------------------------------------------------

def upsert_topas_catalog(conn: Connection, tours: list[dict]) -> tuple[int, int, int]:
    """Replace catalog with fresh data from a refresh.

    Strategy: full replace (truncate + insert), preserving discovered_at for
    tours we've seen before (matched by tour_code, then by URL as fallback).
    This is correct because the catalog represents "what Topas sells right
    now" — stale rows that aren't in the latest refresh should be removed,
    not kept as ghosts.

    Per user requirement: tour_code is the canonical dedup key. The Python
    layer (topas_catalog.fetch_topas_catalog) has already deduplicated on
    tour_code before calling this. This function trusts that input.

    Returns (new_count, updated_count, removed_count):
      new      — tour_code not seen in DB before this refresh
      updated  — tour_code existed; discovered_at preserved, fields refreshed
      removed  — rows that were in DB but not in this refresh (stale)
    """
    now = _now_iso()

    # Snapshot existing data for discovered_at preservation + removal counting
    existing_rows = conn.execute(
        "SELECT tour_code, url, discovered_at FROM topas_catalog"
    ).fetchall()
    existing_by_code = {
        row["tour_code"]: row["discovered_at"]
        for row in existing_rows
        if row["tour_code"]
    }
    existing_by_url = {row["url"]: row["discovered_at"] for row in existing_rows}
    existing_count = len(existing_rows)

    # Compute which tour_codes have competitor mappings (from approved targets)
    try:
        from . import catalog_db as _catdb
        _cat = _catdb.connect()
        mapped_codes = {a["topas_tour_code"] for a in _catdb.list_approved_targets(_cat)}
        _cat.close()
    except Exception:
        mapped_codes = set()

    # Wipe and rebuild — fresh refresh is authoritative
    conn.execute("DELETE FROM topas_catalog")

    new_count = 0
    updated_count = 0

    for t in tours:
        url = t.get("url")
        if not url:
            continue

        tour_code = t.get("tour_code")
        has_mapping = 1 if (tour_code and tour_code in mapped_codes) else 0

        # Preserve discovered_at: prefer match on tour_code, fall back to URL
        if tour_code and tour_code in existing_by_code:
            discovered_at = existing_by_code[tour_code]
            updated_count += 1
        elif url in existing_by_url:
            discovered_at = existing_by_url[url]
            updated_count += 1
        else:
            discovered_at = now
            new_count += 1

        conn.execute(
            """
            INSERT INTO topas_catalog (
                tour_code, tour_name, url, country, duration_days,
                from_price_dkk, audience_segment,
                discovered_at, last_seen_at, has_competitor_mapping
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tour_code,
                t.get("tour_name"),
                url,
                t.get("country"),
                t.get("duration_days"),
                t.get("from_price_dkk"),
                t.get("audience_segment"),
                discovered_at,
                now,
                has_mapping,
            ),
        )

    conn.commit()
    removed_count = existing_count - updated_count
    return new_count, updated_count, removed_count


def add_topas_catalog_entry(conn: Connection, tour: dict) -> str:
    """Insert or update a SINGLE catalog entry without touching the rest.

    Used by the "Tilføj ny tur" / "Re-scrape" workflows where we update one
    row at a time. Distinct from upsert_topas_catalog which truncates and
    rebuilds the whole table.

    Returns 'new' if the URL wasn't in DB before, 'updated' if it was.
    """
    now = _now_iso()
    url = tour.get("url")
    if not url:
        raise ValueError("tour must have a 'url' field")

    tour_code = tour.get("tour_code")

    # Compute mapping flag from approved competitor targets
    try:
        from . import catalog_db as _catdb
        _cat = _catdb.connect()
        mapped_codes = {a["topas_tour_code"] for a in _catdb.list_approved_targets(_cat)}
        _cat.close()
    except Exception:
        mapped_codes = set()
    has_mapping = 1 if (tour_code and tour_code in mapped_codes) else 0

    # Check if URL already exists
    existing = conn.execute(
        "SELECT discovered_at FROM topas_catalog WHERE url = ?",
        (url,),
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE topas_catalog
            SET tour_code = ?,
                tour_name = ?,
                country = ?,
                duration_days = ?,
                from_price_dkk = ?,
                audience_segment = ?,
                last_seen_at = ?,
                has_competitor_mapping = ?
            WHERE url = ?
            """,
            (
                tour_code,
                tour.get("tour_name"),
                tour.get("country"),
                tour.get("duration_days"),
                tour.get("from_price_dkk"),
                tour.get("audience_segment"),
                now,
                has_mapping,
                url,
            ),
        )
        conn.commit()
        return "updated"
    else:
        conn.execute(
            """
            INSERT INTO topas_catalog (
                tour_code, tour_name, url, country, duration_days,
                from_price_dkk, audience_segment,
                discovered_at, last_seen_at, has_competitor_mapping
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tour_code,
                tour.get("tour_name"),
                url,
                tour.get("country"),
                tour.get("duration_days"),
                tour.get("from_price_dkk"),
                tour.get("audience_segment"),
                now,
                now,
                has_mapping,
            ),
        )
        conn.commit()
        return "new"


def fetch_topas_catalog(conn: Connection) -> list[dict]:
    """Return all catalog entries, sorted by country then tour_name."""
    return conn.execute(
        """
        SELECT * FROM topas_catalog
        ORDER BY
            CASE WHEN country IS NULL THEN 1 ELSE 0 END,
            country,
            tour_name
        """
    ).fetchall()
