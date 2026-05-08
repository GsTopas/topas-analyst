"""
Catalog database — persistent store for the AI-discovery layer.

Different from `snapshots.db` (which holds prices and departures). This DB
holds tour-metadata: which tours exist on each operator's site, what they
look like, AI's classifications, and human review decisions.

Schema overview:

  operators              Static list of competitor brands we track.
  catalog_tours          Every tour discovered on every operator site.
                         One row per (operator, tour_url). Tracks active /
                         inactive status across discovery runs.
  tour_extractions       Latest Firecrawl AI-extract for each tour. Cached
                         on a content hash so we don't re-extract unchanged
                         pages.
  tour_classifications   Claude API's classification. Eligibility, activity,
                         difficulty, confidence, and reasoning. Linked to
                         extractions and to the playbook version used.
  match_proposals        Per Topas-tour: AI's ranked list of candidate
                         competitors with tier + reasoning.
  review_decisions       FEEDBACK LOOP. Append-only log of every human
                         review action. The classifier reads recent decisions
                         to inform future runs (see classification_playbook.md).
  pattern_observations   FEEDBACK LOOP. Synthesized rules extracted from
                         the decision log. Feeds the playbook.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import _pg_conn

# Type-alias for læsbarhed — samme PgConnection som db.py bruger.
Connection = _pg_conn.PgConnection


# Catalog DB-stien bevares for kompatibilitet, men bruges ikke længere —
# alle data ligger i Supabase nu.
DEFAULT_CATALOG_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "catalog.db"


# Skemaet ligger i Supabase. SCHEMA bevares som dokumentation men kører ikke
# fra app-koden; ændringer skal gøres centralt i Supabase via apply_migration.
SCHEMA = """
-- Operators we track. Pre-seeded; new entries added as we expand.
CREATE TABLE IF NOT EXISTS operators (
    operator       TEXT PRIMARY KEY,
    holding        TEXT,
    homepage_url   TEXT NOT NULL,
    sitemap_url    TEXT,
    listing_urls   TEXT,             -- JSON array of catalog/listing pages
    notes          TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

-- Every tour we've discovered on every operator's site.
CREATE TABLE IF NOT EXISTS catalog_tours (
    operator       TEXT NOT NULL,
    tour_url       TEXT NOT NULL,
    tour_slug      TEXT NOT NULL,
    discovered_at  TEXT NOT NULL,    -- first time we saw it
    last_seen_at   TEXT NOT NULL,    -- last discovery run that found it
    is_active      INTEGER NOT NULL DEFAULT 1,
    discovery_method TEXT,           -- 'sitemap' | 'listing-crawl'
    PRIMARY KEY (operator, tour_url),
    FOREIGN KEY (operator) REFERENCES operators(operator)
);

-- Latest extracted facts per tour. Cached on content_hash to avoid
-- re-extraction when the page hasn't changed.
CREATE TABLE IF NOT EXISTS tour_extractions (
    operator       TEXT NOT NULL,
    tour_url       TEXT NOT NULL,
    extracted_at   TEXT NOT NULL,
    content_hash   TEXT NOT NULL,    -- hash of the scraped markdown
    -- Structured fields filled by Firecrawl AI-extract:
    title          TEXT,
    duration_days  INTEGER,
    from_price_dkk INTEGER,
    country        TEXT,
    region         TEXT,
    has_fixed_dates    INTEGER,      -- 0/1
    has_published_prices INTEGER,    -- 0/1
    raw_payload    TEXT,             -- full JSON from extractor for audit
    PRIMARY KEY (operator, tour_url),
    FOREIGN KEY (operator, tour_url)
        REFERENCES catalog_tours(operator, tour_url)
);

-- Claude API's classification of each tour. Re-runnable; we keep history
-- via classified_at + playbook_version so we can diff classifier output
-- across rule-changes.
CREATE TABLE IF NOT EXISTS tour_classifications (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    operator          TEXT NOT NULL,
    tour_url          TEXT NOT NULL,
    classified_at     TEXT NOT NULL,
    playbook_version  TEXT,            -- which playbook commit/hash drove this
    -- Verdict fields:
    is_faellesrejse   INTEGER,         -- 0/1; the 5-criteria test
    tour_format       TEXT,            -- Fællesrejse | Individuel | Privat gruppe
    primary_activity  TEXT,
    audience_segment  TEXT,
    difficulty_norm   INTEGER,         -- normalised 1-5
    -- Confidence + reasoning:
    confidence        REAL,            -- 0.0 - 1.0
    reasoning         TEXT,            -- Claude's explanation
    raw_response      TEXT,            -- full JSON for audit
    superseded        INTEGER NOT NULL DEFAULT 0,  -- newer classification exists
    FOREIGN KEY (operator, tour_url)
        REFERENCES catalog_tours(operator, tour_url)
);
CREATE INDEX IF NOT EXISTS idx_classifications_lookup
    ON tour_classifications(operator, tour_url, superseded);

-- Per Topas-tour: AI's ranked competitor matches.
CREATE TABLE IF NOT EXISTS match_proposals (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    topas_tour_code   TEXT NOT NULL,
    proposed_at       TEXT NOT NULL,
    candidate_operator TEXT NOT NULL,
    candidate_url     TEXT NOT NULL,
    proposed_tier     INTEGER,         -- 1-4
    confidence        REAL,
    reasoning         TEXT,
    superseded        INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (candidate_operator, candidate_url)
        REFERENCES catalog_tours(operator, tour_url)
);
CREATE INDEX IF NOT EXISTS idx_proposals_lookup
    ON match_proposals(topas_tour_code, superseded);

-- ===========================================================================
-- FEEDBACK LOOP TABLES
-- ===========================================================================

-- Human review actions on classifications and matches. Append-only —
-- this is the audit trail and the data that pattern-synthesis reads.
CREATE TABLE IF NOT EXISTS review_decisions (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    decided_at        TEXT NOT NULL,
    -- What was being reviewed:
    target_kind       TEXT NOT NULL,    -- 'classification' | 'match'
    target_id         INTEGER NOT NULL, -- FK into tour_classifications or match_proposals
    -- The decision:
    action            TEXT NOT NULL,    -- 'approve' | 'reject' | 'override'
    -- For overrides — the corrected verdict:
    override_payload  TEXT,             -- JSON of corrected fields
    -- Why:
    reason            TEXT NOT NULL,    -- required for reject + override; encouraged for approve
    -- Who:
    reviewer          TEXT              -- analyst id; multi-user support later
);
CREATE INDEX IF NOT EXISTS idx_decisions_target
    ON review_decisions(target_kind, target_id);
CREATE INDEX IF NOT EXISTS idx_decisions_recent
    ON review_decisions(decided_at);

-- n8n competitor screening candidates. Local cache of rows pulled from
-- n8n's "Competitor Analysis" data table. Each row corresponds to one
-- AI-suggested competitor tour for a Topas tour-code.
--
-- Why cached locally: n8n is the source of truth for new screenings, but
-- we want to (a) avoid hitting the webhook on every page render, and
-- (b) integrate with review_decisions where target_kind='n8n_candidate'
-- and target_id=n8n_row_id.
CREATE TABLE IF NOT EXISTS n8n_candidates (
    n8n_row_id        INTEGER PRIMARY KEY,    -- from n8n's data table id
    competitor_domain TEXT,
    topas_tour_code   TEXT,
    search_country    TEXT,
    search_region     TEXT,
    has_match         INTEGER,                -- 0/1
    tour_name         TEXT,
    tour_url          TEXT,
    next_departure    TEXT,
    price             TEXT,
    tour_category     TEXT,                   -- vandre/kultur/kombineret/andet from AI
    duration_days     INTEGER,
    match_confidence  TEXT,                   -- 'high' | 'medium' | 'low' | ''
    notes             TEXT,
    searched_at       TEXT,
    n8n_created_at    TEXT,
    imported_at       TEXT NOT NULL,          -- when WE pulled the row from n8n
    departures_json   TEXT,                   -- JSON array (deprecated; kept for backwards compat)
    has_guide         INTEGER,                -- 0/1: Danish tour leader confirmed
    has_fixed_departures INTEGER              -- 0/1: page shows fixed dates (not inquiry-based)
);
CREATE INDEX IF NOT EXISTS idx_n8n_candidates_tour
    ON n8n_candidates(topas_tour_code);
CREATE INDEX IF NOT EXISTS idx_n8n_candidates_imported
    ON n8n_candidates(imported_at);

-- Approved competitor targets — tells the local scraper what URLs to scrape.
-- Populated automatically when a reviewer Approves an n8n candidate.
-- Removed automatically when a previously approved candidate is Rejected.
-- Source-of-truth for "which competitor tours are we tracking" — replaces the
-- hardcoded TARGETS list in config.py.
CREATE TABLE IF NOT EXISTS approved_competitor_targets (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    operator          TEXT NOT NULL,
    tour_url          TEXT NOT NULL,
    topas_tour_code   TEXT NOT NULL,
    parser_key        TEXT NOT NULL,           -- maps to topas_scraper.parsers.* module
    tour_name         TEXT,                    -- snapshot of name at approval time
    duration_days     INTEGER,                 -- snapshot of duration at approval time
    tour_category     TEXT,                    -- 'vandre' | 'kultur' | 'kombineret' | 'andet'
    approved_at       TEXT NOT NULL,
    approved_by       TEXT,
    decision_id       INTEGER,                 -- FK into review_decisions
    UNIQUE (operator, tour_url, topas_tour_code)
);
CREATE INDEX IF NOT EXISTS idx_approved_targets_tour
    ON approved_competitor_targets(topas_tour_code);
CREATE INDEX IF NOT EXISTS idx_approved_targets_operator
    ON approved_competitor_targets(operator);

-- Patterns extracted from the decision log. Generated on-demand by the
-- "Find mønstre"-knap; humans review and edit before they land in the
-- playbook. This table is a draft / scratch buffer between log and playbook.
CREATE TABLE IF NOT EXISTS pattern_observations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at       TEXT NOT NULL,
    pattern_text      TEXT NOT NULL,    -- proposed rule, in plain Danish
    supporting_decision_ids TEXT,       -- JSON array of review_decision.id
    occurrence_count  INTEGER,
    status            TEXT NOT NULL DEFAULT 'proposed',
        -- 'proposed' | 'accepted' | 'rejected' | 'merged-into-playbook'
    accepted_at       TEXT,
    merged_to_playbook_at TEXT
);
"""


def connect(db_path: Path | str = DEFAULT_CATALOG_DB_PATH) -> Connection:
    """Åbn forbindelse til Supabase Postgres. db_path-argumentet ignoreres
    (bevaret for kompatibilitet)."""
    return _pg_conn.connect()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# _migrate_n8n_candidates fjernet — alle migrations kører nu via Supabase
# apply_migration centralt. App-koden migrerer ikke længere skemaet.


# ---------------------------------------------------------------------------
# Operators
# ---------------------------------------------------------------------------

def upsert_operator(
    conn: Connection,
    operator: str,
    homepage_url: str,
    holding: Optional[str] = None,
    sitemap_url: Optional[str] = None,
    listing_urls: Optional[list[str]] = None,
    notes: Optional[str] = None,
) -> None:
    now = now_iso()
    conn.execute(
        """
        INSERT INTO operators (operator, holding, homepage_url, sitemap_url, listing_urls, notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(operator) DO UPDATE SET
            holding=excluded.holding,
            homepage_url=excluded.homepage_url,
            sitemap_url=excluded.sitemap_url,
            listing_urls=excluded.listing_urls,
            notes=excluded.notes,
            updated_at=excluded.updated_at
        """,
        (operator, holding, homepage_url, sitemap_url,
         json.dumps(listing_urls) if listing_urls else None,
         notes, now, now),
    )
    conn.commit()


def list_operators(conn: Connection) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM operators ORDER BY operator").fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Catalog tours — discovery layer output
# ---------------------------------------------------------------------------

def upsert_catalog_tour(
    conn: Connection,
    operator: str,
    tour_url: str,
    tour_slug: str,
    discovery_method: str,
) -> None:
    """Add a discovered tour, or update last_seen_at if already known."""
    now = now_iso()
    conn.execute(
        """
        INSERT INTO catalog_tours (operator, tour_url, tour_slug, discovered_at, last_seen_at, is_active, discovery_method)
        VALUES (?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(operator, tour_url) DO UPDATE SET
            last_seen_at=excluded.last_seen_at,
            is_active=1,
            discovery_method=excluded.discovery_method
        """,
        (operator, tour_url, tour_slug, now, now, discovery_method),
    )
    conn.commit()


def mark_tours_inactive(
    conn: Connection,
    operator: str,
    seen_urls: list[str],
) -> int:
    """Mark tours as inactive if they didn't appear in this discovery run.
    Returns count of newly-inactive tours."""
    placeholders = ",".join("?" * len(seen_urls)) if seen_urls else "''"
    cursor = conn.execute(
        f"""
        UPDATE catalog_tours
        SET is_active=0
        WHERE operator=? AND is_active=1 AND tour_url NOT IN ({placeholders})
        """,
        [operator] + seen_urls,
    )
    conn.commit()
    return cursor.rowcount


def list_catalog_tours(
    conn: Connection,
    operator: Optional[str] = None,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM catalog_tours WHERE 1=1"
    params: list[Any] = []
    if operator:
        sql += " AND operator=?"
        params.append(operator)
    if active_only:
        sql += " AND is_active=1"
    sql += " ORDER BY operator, tour_slug"
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


# ---------------------------------------------------------------------------
# Extractions
# ---------------------------------------------------------------------------

def upsert_extraction(
    conn: Connection,
    operator: str,
    tour_url: str,
    content_hash: str,
    payload: dict[str, Any],
) -> None:
    """Store the latest Firecrawl extract. Idempotent on content_hash."""
    conn.execute(
        """
        INSERT INTO tour_extractions (operator, tour_url, extracted_at, content_hash,
            title, duration_days, from_price_dkk, country, region,
            has_fixed_dates, has_published_prices, raw_payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(operator, tour_url) DO UPDATE SET
            extracted_at=excluded.extracted_at,
            content_hash=excluded.content_hash,
            title=excluded.title,
            duration_days=excluded.duration_days,
            from_price_dkk=excluded.from_price_dkk,
            country=excluded.country,
            region=excluded.region,
            has_fixed_dates=excluded.has_fixed_dates,
            has_published_prices=excluded.has_published_prices,
            raw_payload=excluded.raw_payload
        """,
        (
            operator, tour_url, now_iso(), content_hash,
            payload.get("title"),
            payload.get("duration_days"),
            payload.get("from_price_dkk"),
            payload.get("country"),
            payload.get("region"),
            int(bool(payload.get("has_fixed_dates"))) if payload.get("has_fixed_dates") is not None else None,
            int(bool(payload.get("has_published_prices"))) if payload.get("has_published_prices") is not None else None,
            json.dumps(payload),
        ),
    )
    conn.commit()


def get_extraction(conn: Connection, operator: str, tour_url: str) -> Optional[dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM tour_extractions WHERE operator=? AND tour_url=?",
        (operator, tour_url),
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Classifications
# ---------------------------------------------------------------------------

def insert_classification(
    conn: Connection,
    operator: str,
    tour_url: str,
    playbook_version: str,
    verdict: dict[str, Any],
) -> int:
    """Mark all previous classifications for this tour as superseded, then
    insert the new one. Returns the new classification id."""
    conn.execute(
        "UPDATE tour_classifications SET superseded=1 WHERE operator=? AND tour_url=? AND superseded=0",
        (operator, tour_url),
    )
    cursor = conn.execute(
        """
        INSERT INTO tour_classifications (
            operator, tour_url, classified_at, playbook_version,
            is_faellesrejse, tour_format, primary_activity, audience_segment, difficulty_norm,
            confidence, reasoning, raw_response, superseded
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        RETURNING id
        """,
        (
            operator, tour_url, now_iso(), playbook_version,
            int(bool(verdict.get("is_faellesrejse"))) if verdict.get("is_faellesrejse") is not None else None,
            verdict.get("tour_format"),
            verdict.get("primary_activity"),
            verdict.get("audience_segment"),
            verdict.get("difficulty_norm"),
            verdict.get("confidence"),
            verdict.get("reasoning"),
            json.dumps(verdict),
        ),
    )
    new_id = cursor.lastrowid
    conn.commit()
    return new_id


def get_active_classification(
    conn: Connection,
    operator: str,
    tour_url: str,
) -> Optional[dict[str, Any]]:
    row = conn.execute(
        """
        SELECT * FROM tour_classifications
        WHERE operator=? AND tour_url=? AND superseded=0
        ORDER BY classified_at DESC LIMIT 1
        """,
        (operator, tour_url),
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Review decisions — FEEDBACK LOOP
# ---------------------------------------------------------------------------

def log_review_decision(
    conn: Connection,
    target_kind: str,           # 'classification' | 'match'
    target_id: int,
    action: str,                # 'approve' | 'reject' | 'override'
    reason: str,
    override_payload: Optional[dict[str, Any]] = None,
    reviewer: Optional[str] = None,
) -> int:
    """Append a review action to the decision log."""
    if action in ("reject", "override") and not reason.strip():
        raise ValueError(f"Reason is required for {action} actions")

    cursor = conn.execute(
        """
        INSERT INTO review_decisions (
            decided_at, target_kind, target_id, action,
            override_payload, reason, reviewer
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        RETURNING id
        """,
        (
            now_iso(), target_kind, target_id, action,
            json.dumps(override_payload) if override_payload else None,
            reason, reviewer,
        ),
    )
    new_id = cursor.lastrowid
    conn.commit()
    return new_id


def fetch_recent_decisions(
    conn: Connection,
    limit: int = 50,
    only_overrides: bool = False,
) -> list[dict[str, Any]]:
    """Read recent review decisions. Used by:
    1. The classifier — to read recent overrides as context
    2. The pattern-synthesis routine — to find recurring corrections"""
    sql = "SELECT * FROM review_decisions"
    if only_overrides:
        sql += " WHERE action IN ('reject', 'override')"
    sql += " ORDER BY decided_at DESC LIMIT ?"
    rows = conn.execute(sql, (limit,)).fetchall()
    return [dict(r) for r in rows]


def fetch_decisions_for_target(
    conn: Connection,
    target_kind: str,
    target_id: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM review_decisions WHERE target_kind=? AND target_id=? ORDER BY decided_at DESC",
        (target_kind, target_id),
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Pattern observations — output of "Find mønstre" routine (Session 2)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Pattern observations — output of "Find mønstre" routine (Session 2)
# ---------------------------------------------------------------------------

def insert_pattern_observation(
    conn: Connection,
    pattern_text: str,
    supporting_decision_ids: list[int],
    occurrence_count: int,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO pattern_observations (
            observed_at, pattern_text, supporting_decision_ids, occurrence_count, status
        ) VALUES (?, ?, ?, ?, 'proposed')
        RETURNING id
        """,
        (now_iso(), pattern_text, json.dumps(supporting_decision_ids), occurrence_count),
    )
    new_id = cursor.lastrowid
    conn.commit()
    return new_id


def list_pattern_observations(
    conn: Connection,
    status: "Optional[str]" = None,
) -> list[dict[str, "Any"]]:
    sql = "SELECT * FROM pattern_observations"
    params: list = []
    if status:
        sql += " WHERE status=?"
        params.append(status)
    sql += " ORDER BY observed_at DESC"
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def update_pattern_status(
    conn: Connection,
    pattern_id: int,
    status: str,
) -> None:
    field = "accepted_at" if status == "accepted" else (
        "merged_to_playbook_at" if status == "merged-into-playbook" else None
    )
    if field:
        conn.execute(
            f"UPDATE pattern_observations SET status=?, {field}=? WHERE id=?",
            (status, now_iso(), pattern_id),
        )
    else:
        conn.execute(
            "UPDATE pattern_observations SET status=? WHERE id=?",
            (status, pattern_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# n8n candidates — competitor screening rows pulled from n8n
# ---------------------------------------------------------------------------

def upsert_n8n_candidate(conn: Connection, row: dict) -> bool:
    """Insert or update one candidate from n8n. Returns True if newly inserted.

    `row` is a dict shaped like an n8n data table row (keys: id,
    competitorDomain, topasTourCode, hasMatch, tourName, tourUrl,
    nextDeparture, price, durationDays, matchConfidence, notes,
    searchedAt, createdAt, ...). Missing keys are treated as None.
    """
    n8n_id = row.get("id")
    if n8n_id is None:
        raise ValueError("Candidate row is missing 'id'")

    existing = conn.execute(
        "SELECT 1 FROM n8n_candidates WHERE n8n_row_id=?", (n8n_id,)
    ).fetchone()

    has_match_val = row.get("hasMatch")
    has_match_int = 1 if has_match_val else 0 if has_match_val is not None else None

    duration_val = row.get("durationDays")
    try:
        duration_int = int(duration_val) if duration_val is not None else None
    except (TypeError, ValueError):
        duration_int = None

    conn.execute(
        """
        INSERT INTO n8n_candidates (
            n8n_row_id, competitor_domain, topas_tour_code,
            search_country, search_region, has_match,
            tour_name, tour_url, next_departure, price,
            duration_days, match_confidence, notes,
            searched_at, n8n_created_at, imported_at,
            departures_json, has_guide, has_fixed_departures,
            tour_category
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(n8n_row_id) DO UPDATE SET
            competitor_domain=excluded.competitor_domain,
            topas_tour_code=excluded.topas_tour_code,
            search_country=excluded.search_country,
            search_region=excluded.search_region,
            has_match=excluded.has_match,
            tour_name=excluded.tour_name,
            tour_url=excluded.tour_url,
            next_departure=excluded.next_departure,
            price=excluded.price,
            duration_days=excluded.duration_days,
            match_confidence=excluded.match_confidence,
            notes=excluded.notes,
            searched_at=excluded.searched_at,
            n8n_created_at=excluded.n8n_created_at,
            departures_json=excluded.departures_json,
            has_guide=excluded.has_guide,
            has_fixed_departures=excluded.has_fixed_departures,
            tour_category=excluded.tour_category
        """,
        (
            n8n_id,
            row.get("competitorDomain"),
            row.get("topasTourCode"),
            row.get("searchCountry"),
            row.get("searchRegion"),
            has_match_int,
            row.get("tourName"),
            row.get("tourUrl"),
            row.get("nextDeparture"),
            row.get("price"),
            duration_int,
            row.get("matchConfidence"),
            row.get("notes"),
            row.get("searchedAt"),
            row.get("createdAt"),
            now_iso(),
            row.get("departures"),
            _bool_to_int(row.get("hasGuide")),
            _bool_to_int(row.get("hasFixedDepartures")),
            (row.get("tourCategory") or "").lower() or None,
        ),
    )
    conn.commit()
    return existing is None


def _bool_to_int(v):
    """Coerce truthy-ish values into 0/1/None for SQLite booleans."""
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, (int, float)):
        return 1 if v else 0
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes", "ja"):
            return 1
        if s in ("false", "0", "no", "nej"):
            return 0
    return None


def list_n8n_candidates_for_tour(
    conn: Connection,
    topas_tour_code: str,
    only_unreviewed: bool = False,
) -> list[dict]:
    """List n8n candidates for a given Topas tour, optionally filtering to
    those without a review_decision yet."""
    # Skip altid rækker uden tour_url — de er "konkurrent screenet, intet
    # match fundet" og har intet at reviewe på (kun en note om hvorfor).
    # De vises separat i en sammenfatning i UI'et.
    url_filter = "AND c.tour_url IS NOT NULL AND c.tour_url != ''"
    if only_unreviewed:
        sql = f"""
            SELECT c.* FROM n8n_candidates c
            WHERE c.topas_tour_code=?
              {url_filter}
              AND NOT EXISTS (
                SELECT 1 FROM review_decisions d
                JOIN n8n_candidates c2 ON d.target_id = c2.n8n_row_id
                WHERE d.target_kind = 'n8n_candidate'
                  AND c2.tour_url = c.tour_url
                  AND c2.topas_tour_code = c.topas_tour_code
              )
            ORDER BY
                CASE c.match_confidence
                    WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 ELSE 3
                END,
                c.competitor_domain, c.tour_name
        """
    else:
        sql = f"""
            SELECT c.* FROM n8n_candidates c
            WHERE c.topas_tour_code=?
              {url_filter}
            ORDER BY
                CASE c.match_confidence
                    WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 ELSE 3
                END,
                c.competitor_domain, c.tour_name
        """
    rows = conn.execute(sql, (topas_tour_code,)).fetchall()
    return [dict(r) for r in rows]


def list_n8n_tour_codes(conn: Connection) -> list[dict]:
    """List Topas tour-codes that have any n8n candidates, with counts of
    total / unreviewed candidates per tour. Used to populate review-page
    selector with badge."""
    rows = conn.execute(
        """
        SELECT
            c.topas_tour_code AS tour_code,
            COUNT(*) AS total,
            SUM(CASE WHEN d.id IS NULL THEN 1 ELSE 0 END) AS unreviewed
        FROM n8n_candidates c
        LEFT JOIN review_decisions d
            ON d.target_kind='n8n_candidate' AND d.target_id=c.n8n_row_id
        WHERE c.topas_tour_code IS NOT NULL AND c.topas_tour_code != ''
        GROUP BY c.topas_tour_code
        ORDER BY unreviewed DESC, tour_code
        """
    ).fetchall()
    return [dict(r) for r in rows]


def get_n8n_candidate_decision(conn: Connection, n8n_row_id: int):
    """Return the latest review_decision for this candidate, if any."""
    row = conn.execute(
        """
        SELECT * FROM review_decisions
        WHERE target_kind='n8n_candidate' AND target_id=?
        ORDER BY decided_at DESC LIMIT 1
        """,
        (n8n_row_id,),
    ).fetchone()
    return dict(row) if row else None


def get_decision_for_url(
    conn: Connection,
    tour_url: str,
    topas_tour_code: str,
):
    """Return the latest review_decision for ANY n8n_candidate matching
    (tour_url, topas_tour_code) — uanset n8n_row_id.

    Bruges fordi re-screening laver nye rows med nye id'er. Vi vil tracke
    decisions per URL, ikke per n8n_row_id."""
    row = conn.execute(
        """
        SELECT d.*
        FROM review_decisions d
        JOIN n8n_candidates c ON d.target_id = c.n8n_row_id
        WHERE d.target_kind = 'n8n_candidate'
          AND c.tour_url = ?
          AND c.topas_tour_code = ?
        ORDER BY d.decided_at DESC
        LIMIT 1
        """,
        (tour_url, topas_tour_code),
    ).fetchone()
    return dict(row) if row else None

def list_latest_n8n_candidates_for_tour(
    conn: Connection,
    topas_tour_code: str,
    only_unreviewed: bool = False,
) -> list[dict]:
    """Return only the LATEST candidate per (competitor_domain, tour_url) for
    a given Topas tour. Older screening runs of the same competitor tour are
    hidden. No-match rows are deduped per competitor_domain.

    Used by the review page to avoid showing stale data from earlier screening
    iterations that hallucinated dates/prices.
    """
    # Skip altid rækker uden tour_url — ingen actionable info.
    if only_unreviewed:
        sql = """
            WITH latest AS (
                SELECT MAX(n8n_row_id) AS row_id
                FROM n8n_candidates
                WHERE topas_tour_code=?
                  AND tour_url IS NOT NULL AND tour_url != ''
                GROUP BY
                    competitor_domain,
                    tour_url
            )
            SELECT c.* FROM n8n_candidates c
            INNER JOIN latest l ON l.row_id = c.n8n_row_id
            WHERE NOT EXISTS (
                SELECT 1 FROM review_decisions d
                JOIN n8n_candidates c2 ON d.target_id = c2.n8n_row_id
                WHERE d.target_kind = 'n8n_candidate'
                  AND c2.tour_url = c.tour_url
                  AND c2.topas_tour_code = c.topas_tour_code
            )
            ORDER BY
                CASE c.match_confidence
                    WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 ELSE 3
                END,
                c.competitor_domain, c.tour_name
        """
    else:
        sql = """
            WITH latest AS (
                SELECT MAX(n8n_row_id) AS row_id
                FROM n8n_candidates
                WHERE topas_tour_code=?
                  AND tour_url IS NOT NULL AND tour_url != ''
                GROUP BY
                    competitor_domain,
                    tour_url
            )
            SELECT c.* FROM n8n_candidates c
            INNER JOIN latest l ON l.row_id = c.n8n_row_id
            ORDER BY
                CASE c.match_confidence
                    WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 ELSE 3
                END,
                c.competitor_domain, c.tour_name
        """
    rows = conn.execute(sql, (topas_tour_code,)).fetchall()
    return [dict(r) for r in rows]


def list_latest_n8n_tour_codes(conn: Connection) -> list[dict]:
    """Like list_n8n_tour_codes but counts only the latest row per (domain, url).
    Mirrors list_latest_n8n_candidates_for_tour for consistent counts in UI.

    Unreviewed-tællingen bruger URL-baseret check (ikke n8n_row_id), så
    re-screenings af tidligere afviste URLs ikke lyver om "pending"-tal.

    Postgres-strict-fix: splittet i to CTEs — først finder vi MAX(n8n_row_id)
    pr. (topas_tour_code, competitor_domain, COALESCE(...)) gruppe; derefter
    joiner vi tilbage for at hente topas_tour_code + tour_url. Det undgår
    'must appear in GROUP BY clause'-fejlen i strict-mode SQL."""
    rows = conn.execute(
        """
        WITH latest_ids AS (
            SELECT MAX(n8n_row_id) AS row_id
            FROM n8n_candidates
            WHERE topas_tour_code IS NOT NULL AND topas_tour_code != ''
            GROUP BY
                topas_tour_code,
                competitor_domain,
                COALESCE(NULLIF(tour_url, ''), '__no_match__')
        ),
        latest AS (
            SELECT n.n8n_row_id AS row_id, n.topas_tour_code, n.tour_url
            FROM n8n_candidates n
            INNER JOIN latest_ids li ON li.row_id = n.n8n_row_id
        )
        SELECT
            l.topas_tour_code AS tour_code,
            COUNT(*) AS total,
            SUM(
                CASE WHEN NOT EXISTS (
                    SELECT 1 FROM review_decisions d
                    JOIN n8n_candidates c2 ON d.target_id = c2.n8n_row_id
                    WHERE d.target_kind = 'n8n_candidate'
                      AND c2.tour_url = l.tour_url
                      AND c2.topas_tour_code = l.topas_tour_code
                ) THEN 1 ELSE 0 END
            ) AS unreviewed
        FROM latest l
        GROUP BY l.topas_tour_code
        ORDER BY unreviewed DESC, tour_code
        """
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Approved competitor targets — auto-managed from review decisions
# ---------------------------------------------------------------------------

DOMAIN_TO_PARSER_KEY = {
    "albatros.dk": "albatros",
    "besttravel.dk": "besttravel",
    "fyrholtrejser.dk": "fyrholt",            # NB: uden bindestreg
    "gjoa.dk": "gjoa",
    "jysk-rejsebureau.dk": "jysk",
    "kiplingtravel.dk": "kipling",
    "nillesgislev.dk": "nillesgislev",
    "ruby-rejser.dk": "ruby",
    "smilrejser.dk": "smilrejser",
    "stjernegaard-rejser.dk": "stjernegaard",
    "vagabondtours.dk": "vagabond",
    "viktorsfarmor.dk": "viktorsfarmor",
}


def parser_key_for_domain(domain: str) -> str:
    """Map competitor domain to parser key. Falls back to 'generic_ai' for
    unknown operators — the generic AI extractor handles those."""
    if not domain:
        return "generic_ai"
    d = domain.lower().strip().replace("https://", "").replace("http://", "").rstrip("/")
    if d.startswith("www."):
        d = d[4:]
    return DOMAIN_TO_PARSER_KEY.get(d, "generic_ai")


def list_approved_targets(
    conn: Connection,
    topas_tour_code: Optional[str] = None,
) -> list[dict]:
    """Liste godkendte konkurrent-targets — bruges af scraper og UI.

    Hvis topas_tour_code er givet, filtreres til kun den tour-kode.
    Ellers returneres alle på tværs af alle tours.
    Sortering: tour_code, operator, tour_name."""
    if topas_tour_code:
        rows = conn.execute(
            """
            SELECT *
            FROM approved_competitor_targets
            WHERE topas_tour_code = ?
            ORDER BY operator, tour_name
            """,
            (topas_tour_code,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM approved_competitor_targets
            ORDER BY topas_tour_code, operator, tour_name
            """
        ).fetchall()
    return [dict(r) for r in rows]


def upsert_approved_target(
    conn: Connection,
    operator: str,
    tour_url: str,
    topas_tour_code: str,
    tour_name: Optional[str] = None,
    duration_days: Optional[int] = None,
    tour_category: Optional[str] = None,
    approved_by: Optional[str] = None,
    decision_id: Optional[int] = None,
) -> bool:
    """Add or refresh an approved competitor target. Returns True if newly inserted.
    
    tour_category — when provided AND the target is new OR currently has no
    category, this value is set. If the target already has a non-empty
    category, it's NOT overwritten (preserves manual user edits)."""
    parser_key = parser_key_for_domain(operator)
    existing_row = conn.execute(
        "SELECT id, tour_category FROM approved_competitor_targets WHERE operator=? AND tour_url=? AND topas_tour_code=?",
        (operator, tour_url, topas_tour_code),
    ).fetchone()
    existing = existing_row is not None

    # Decide what category to write
    final_category = tour_category
    if existing_row and existing_row["tour_category"]:
        # Preserve user's existing category (don't overwrite manual edits)
        final_category = existing_row["tour_category"]

    conn.execute(
        """
        INSERT INTO approved_competitor_targets (
            operator, tour_url, topas_tour_code, parser_key,
            tour_name, duration_days, tour_category,
            approved_at, approved_by, decision_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(operator, tour_url, topas_tour_code) DO UPDATE SET
            parser_key=excluded.parser_key,
            tour_name=excluded.tour_name,
            duration_days=excluded.duration_days,
            tour_category=COALESCE(excluded.tour_category, approved_competitor_targets.tour_category),
            approved_at=excluded.approved_at,
            approved_by=excluded.approved_by,
            decision_id=excluded.decision_id
        """,
        (
            operator, tour_url, topas_tour_code, parser_key,
            tour_name, duration_days, final_category,
            now_iso(), approved_by, decision_id,
        ),
    )
    conn.commit()
    return not existing


def delete_approved_target(
    conn: Connection,
    operator: str,
    tour_url: str,
    topas_tour_code: str,
) -> bool:
    """Slet en godkendt target. Returnerer True hvis en række blev slettet."""
    cur = conn.execute(
        "DELETE FROM approved_competitor_targets WHERE operator=? AND tour_url=? AND topas_tour_code=?",
        (operator, tour_url, topas_tour_code),
    )
    conn.commit()
    return cur.rowcount > 0


def update_approved_target_category(
    conn: Connection,
    *,
    target_id: int,
    tour_category: str | None,
) -> None:
    """Opdater kun tour_category for en bestemt godkendt target (by id)."""
    cat_val = (tour_category or "").lower() or None
    conn.execute(
        "UPDATE approved_competitor_targets SET tour_category=? WHERE id=?",
        (cat_val, target_id),
    )
    conn.commit()
