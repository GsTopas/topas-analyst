"""
JSON export — produces the file the dashboard reads.

Multi-tour aware: handles multiple Topas anchors per scraper run and groups
each anchor's competitors by `country` so the dashboard can show per-tour
competitor sets correctly.

Tier classification still lives in this module as a manual map (per-tour-code).
This is where v0.6 §3 tier assignment lives until the methodology layer is
computed automatically.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from .config import DEFAULT_EXPORT_PATH, OPERATOR_META
from .db import connect, fetch_tours, fetch_departures, latest_run_id, get_price_change, detect_status_anomaly
from . import catalog_db, _pg_conn

# Type-aliases bevarer kompatibilitet med tidligere Row / Connection
# annotations uden at de skal opdateres mange steder. Runtime-typen er
# PgConnection (fra _pg_conn) og dict (fra DictCursor).
Connection = _pg_conn.PgConnection
Row = dict


def _approved_set() -> set[tuple[str, str]]:
    """Return set of (url, topas_tour_code) for currently approved competitors.
    Used to filter out historical tours from operators no longer approved.
    Returns empty set if catalog.db doesn't exist yet."""
    try:
        cat = catalog_db.connect()
        rows = catalog_db.list_approved_targets(cat)
        cat.close()
        return {(r["tour_url"], r["topas_tour_code"]) for r in rows if r.get("tour_url") and r.get("topas_tour_code")}
    except Exception:
        return set()


# Tier classification per Topas tour-code → operator → tier.
# Add new entries as tours are added.
TIER_BY_TOUR = {
    "PTMD": {
        "Smilrejser": 1,
        "Jysk Rejsebureau": 1,
        "Viktors Farmor": 1,
        "Ruby Rejser": 2,
        "Nilles & Gislev": 2,
        "Best Travel": 3,  # Kulturrejse, ikke vandring — methodologisk T3 / borderline ineligible
    },
    "NPAP": {
        # Nepal Annapurna — taxonomy v0.5 noted Tier 1 is empty; Stjernegaard +
        # Viktors Farmor are Tier 2 (kultur-led vs Topas trek-led).
        "Viktors Farmor": 2,
        "Stjernegaard Rejser": 2,
        "Albatros Travel": 2,
    },
    "VNSN": {
        # Vietnam — Stjernegaard has multiple Vietnam Fællesrejser. All kultur-led
        # rundrejser, Tier 2 vs Topas's multi-aktivitet rundrejse.
        "Stjernegaard Rejser": 2,
        "Albatros Travel": 2,
    },
}

# Per-tour-code, which operator we prefer as the headline anchor.
ANCHOR_PREF_BY_TOUR = {
    "PTMD": ["Smilrejser", "Jysk Rejsebureau", "Viktors Farmor"],
    "NPAP": ["Viktors Farmor", "Stjernegaard Rejser", "Albatros Travel"],
    "VNSN": ["Stjernegaard Rejser", "Albatros Travel"],
}


def export(db_path: Optional[Path] = None, output: Optional[Path] = None) -> Path:
    """Read SQLite and write dashboard-shape JSON.

    Multi-tour aware: emits one entry per Topas anchor and groups each
    anchor's competitors by tour_code (competes_with).

    Cross-run aggregation: reads ALL tours from the `tours` table — each row
    has its own `last_seen_run` (the table is upserted, not append-only), so
    a per-tour scrape only refreshes that tour. Tours not touched in the
    latest run keep their previous data instead of being wiped.

    Departures are filtered by each tour's own `last_seen_run` so we don't
    show stale departures that were removed from the competitor's page.
    """
    out_path = Path(output) if output else DEFAULT_EXPORT_PATH
    conn = connect(db_path)

    # Use latest_run_id only for the snapshot bookkeeping at the top of
    # the JSON. Per-tour data uses each tour's own last_seen_run below.
    run_id = latest_run_id(conn)
    if not run_id:
        raise RuntimeError("No scraper run found. Run `python -m topas_scraper.cli scrape` first.")

    # Fetch ALL tours (across all runs — each row's last_seen_run reflects
    # the most recent scrape for that operator+slug pair, by UPSERT).
    rows = fetch_tours(conn)

    # Find ALL Topas anchors (PTMD, NPAP, VNSN, etc.) — across runs.
    # Deduplicate by tour_code: if same code has multiple slug-variants
    # (e.g. URL changed format over time), keep only the most recently scraped
    # one. Otherwise the loop below would emit each competitor N times.
    topas_rows_raw = [r for r in rows if r["operator"] == "Topas"]
    if not topas_rows_raw:
        raise RuntimeError("No Topas tour in any run.")

    # Build run_id -> started_at lookup. last_seen_run er en UUID (random hex)
    # som ikke kan sammenlignes lexicographisk for at finde det "nyeste" run —
    # fx "b8abbf74..." > "c3b3e962..." er False fordi 'b' < 'c', selvom b-run'et
    # blev kørt SENERE i tid. Vi joiner med scraper_runs.started_at i stedet.
    run_started: dict[str, str] = {
        r["run_id"]: (r["started_at"] or "")
        for r in conn.execute(
            "SELECT run_id, started_at FROM scraper_runs"
        ).fetchall()
    }

    def _run_ts(row) -> str:
        """Returner started_at for row's last_seen_run, eller '' hvis ukendt."""
        return run_started.get(row["last_seen_run"] or "", "")

    by_code: dict[str, Row] = {}
    for r in topas_rows_raw:
        code = r["tour_code"] or "UNKNOWN"
        existing = by_code.get(code)
        if existing is None or _run_ts(r) > _run_ts(existing):
            by_code[code] = r
    topas_rows = list(by_code.values())

    # De-dup competitors by URL (canonical identifier) per tour-code.
    # Without this, "Viktors Farmor" (legacy parser) and "Viktorsfarmor"
    # (domain-derived from new pipeline) end up as two rows for the same URL,
    # showing as duplicates in the UI. Keep the row with the freshest run
    # (by started_at, not UUID lexicographic).
    competitor_rows_all_raw = [r for r in rows if r["operator"] != "Topas"]
    comp_seen: dict[tuple, Row] = {}
    for r in competitor_rows_all_raw:
        # Key on (url, competes_with) — same URL competing with same Topas tour
        # is by definition the same product, regardless of operator-name spelling.
        key = (r["url"], r["competes_with"])
        existing = comp_seen.get(key)
        if existing is None or _run_ts(r) > _run_ts(existing):
            comp_seen[key] = r

    # Filter: kun konkurrenter der er CURRENTLY godkendt i catalog.db.
    # Uden dette dukker historiske scrapes (fx fra dengang Jysk havde en
    # PTMD-tour vi sidenhen fjernede fra godkendelser) op i UI'et.
    approved = _approved_set()
    if approved:
        competitor_rows_all = [
            r for r in comp_seen.values()
            if (r["url"], r["competes_with"]) in approved
        ]
    else:
        # Fallback: hvis catalog.db er tom/utilgængelig, vis alt
        competitor_rows_all = list(comp_seen.values())

    # Prefetch ALLE snapshots i én query for at undgå N+1 problem.
    # _departure_with_delta læser fra _SNAPSHOTS_CACHE i stedet for at lave
    # 500+ individuelle queries til snapshots-tabellen. Markant hurtigere
    # over netværk (~5-10s → ~1-2s for cold-load).
    global _SNAPSHOTS_CACHE
    _SNAPSHOTS_CACHE = _prefetch_snapshots(conn)

    tour_records = []
    competitor_records = []

    for topas_row in topas_rows:
        tour_code = topas_row["tour_code"] or "UNKNOWN"
        country = topas_row["country"]

        # Competitors for this tour: rows whose competes_with field matches
        # this Topas tour's tour_code. This is the AUTHORITATIVE relationship,
        # not country (which fails when a country has multiple Topas tours).
        # Falls back to country-match for backward-compat with rows scraped
        # before competes_with was added.
        comps_for_tour = [
            r for r in competitor_rows_all
            if (r["competes_with"] == tour_code)
               or (r["competes_with"] is None and r["country"] == country)
        ]

        # KEEP ALL DEPARTURES — including historical ones no longer on the
        # operator's page. Each departure gets isArchived=true if its
        # last_seen_run != tour's latest run. The UI/report layer can then
        # filter for display, but the data stays preserved for time-series
        # analysis (sellout patterns, price evolution, etc.).
        topas_run = topas_row["last_seen_run"]
        topas_deps = list(fetch_departures(conn, "Topas", topas_row["tour_slug"]))
        # Active subset used for metrics only — gamle priser skal ikke
        # skævvride aktuelle gennemsnit/min.
        topas_deps_active = [d for d in topas_deps if d["last_seen_run"] == topas_run]
        topas_per_dep_avg = _avg_price([d["price_dkk"] for d in topas_deps_active])
        topas_min_price = _min_price([d["price_dkk"] for d in topas_deps_active]) or topas_row["from_price_dkk"]

        anchor = _select_anchor(comps_for_tour, tour_code)

        if anchor:
            anchor_run = anchor["last_seen_run"]
            # Anchor-deps brugt KUN til metric-beregning (avg, min) — så vi
            # holder denne filtreret til kun aktive afgange. Den fulde liste
            # eksporteres separat via competitor_records nedenfor.
            anchor_deps = [
                d for d in fetch_departures(conn, anchor["operator"], anchor["tour_slug"])
                if d["last_seen_run"] == anchor_run
            ]
            anchor_per_dep_avg = _avg_price([d["price_dkk"] for d in anchor_deps])
            anchor_min_price = _min_price([d["price_dkk"] for d in anchor_deps]) or anchor["from_price_dkk"]
            all_from = [
                (r["operator"], r["from_price_dkk"])
                for r in [topas_row] + comps_for_tour
                if r["from_price_dkk"]
            ]
            all_from.sort(key=lambda x: x[1])
            topas_rank = next((i + 1 for i, (op, _) in enumerate(all_from) if op == "Topas"), None)
            eligible_size = len(all_from)
        else:
            anchor_deps = []
            anchor_per_dep_avg = None
            anchor_min_price = None
            topas_rank = 1
            eligible_size = 1

        flags = _compute_flags(topas_deps, anchor_deps)

        topas_record = {
            "code": tour_code,
            "name": topas_row["tour_name"],
            "country": country,
            "durationDays": topas_row["duration_days"],
            "mealsIncluded": topas_row["meals_included"] if "meals_included" in topas_row.keys() else None,
            "mealsDescription": topas_row["meals_description"] if "meals_description" in topas_row.keys() else None,
            "audience": "Åben",
            "competitor": {
                "operator": anchor["operator"],
                "tier": TIER_BY_TOUR.get(tour_code, {}).get(anchor["operator"], 2),
                "name": anchor["tour_name"],
            } if anchor else None,
            "topasFromPrice": topas_min_price,
            "competitorFromPrice": anchor_min_price,
            "topasPerDep": topas_per_dep_avg,
            "competitorPerDep": anchor_per_dep_avg,
            "topasFromRank": topas_rank,
            "topasPerDepRank": topas_rank,
            "eligibleSetSize": eligible_size,
            "flags": flags,
            "scraped": True,
            "scrapedAt": run_id,
            "departures": [
                _departure_with_delta(conn, "Topas", topas_row["tour_slug"], d, tour_run=topas_run, fields=("endDate", "departureCode"))
                for d in topas_deps
            ],
        }
        tour_records.append(topas_record)

        # Build competitor records for THIS tour
        tier_map = TIER_BY_TOUR.get(tour_code, {})
        for r in comps_for_tour:
            r_run = r["last_seen_run"]
            # KEEP ALL — historiske afgange bevares med isArchived-flag
            deps = list(fetch_departures(conn, r["operator"], r["tour_slug"]))
            deps_active = [d for d in deps if d["last_seen_run"] == r_run]
            meta = OPERATOR_META.get(r["operator"], {})
            competitor_records.append({
                "operator": r["operator"],
                "holding": meta.get("holding"),
                "segment": meta.get("segment"),
                "tourName": r["tour_name"],
                "tourSlug": r["tour_slug"],
                "url": r["url"],
                "country": r["country"],
                "competesWith": tour_code,
                "durationDays": r["duration_days"],
                "fromPrice": r["from_price_dkk"],
                "eligibilityNotes": r["eligibility_notes"],
                "mealsIncluded": r["meals_included"] if "meals_included" in r.keys() else None,
                "mealsDescription": r["meals_description"] if "meals_description" in r.keys() else None,
                "perDepAvg": _avg_price([d["price_dkk"] for d in deps_active]),
                "departureCount": len(deps_active),
                "departureCountTotal": len(deps),  # incl. arkiv
                "departures": [
                    _departure_with_delta(conn, r["operator"], r["tour_slug"], d, tour_run=r_run, fields=("rejseleder", "flightOrigin"))
                    for d in deps
                ],
                # Keep tierForPTMD field name for backward compat with existing
                # dashboard pages, but compute per-tour
                "tierForPTMD": tier_map.get(r["operator"], 3),
            })

    payload = {
        "snapshotRunId": run_id,
        "snapshotTakenAt": _run_started(conn, run_id),
        "tours": tour_records,
        "competitors": competitor_records,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return out_path


# --- Snapshot prefetch helpers (perf-fix for N+1 query problem) ---

# Module-level cache for prefetched snapshots. Bygges én gang pr. export()-kald
# i stedet for 500+ individuelle queries. Keyed by (operator, tour_slug, start_date)
# → list of snapshot dicts sorted by observed_at DESC.
_SNAPSHOTS_CACHE: dict = {}


def _prefetch_snapshots(conn: Connection) -> dict:
    """Hent ALLE snapshots fra DB i én query. Returner dict keyed på
    (operator, tour_slug, start_date) → list af snapshot-rækker sorteret
    efter observed_at DESC.

    Erstatter ~500 individuelle queries (get_price_change + detect_status_anomaly
    per departure) med 1 stor query → markant hurtigere load_data().
    """
    rows = conn.execute(
        """
        SELECT operator, tour_slug, start_date, price_dkk,
               availability_status, observed_at, run_id
        FROM snapshots
        ORDER BY observed_at DESC
        """
    ).fetchall()
    out: dict = {}
    for r in rows:
        key = (r["operator"], r["tour_slug"], r["start_date"])
        out.setdefault(key, []).append({
            "price_dkk": r["price_dkk"],
            "availability_status": r["availability_status"],
            "observed_at": r["observed_at"],
            "run_id": r["run_id"],
        })
    return out


def _get_price_change_from_list(snapshots: list, lookback_days: int = 7) -> Optional[dict]:
    """Pris-ændring beregnet fra præ-fetchet snapshots-liste. Samme logik som
    db.get_price_change() men uden DB-query."""
    from datetime import datetime, timedelta  # noqa: PLC0415

    valid = [s for s in snapshots if s.get("price_dkk") is not None]
    if len(valid) < 2:
        return None

    latest = valid[0]
    latest_price = latest["price_dkk"]
    latest_observed = latest["observed_at"]
    try:
        latest_dt = datetime.fromisoformat(
            latest_observed.replace("Z", "+00:00").replace("+00:00", "")
        )
    except (ValueError, AttributeError):
        return None

    cutoff = latest_dt - timedelta(days=lookback_days)
    previous = None
    for s in valid[1:]:
        obs = s.get("observed_at")
        if not obs:
            continue
        try:
            s_dt = datetime.fromisoformat(obs.replace("Z", "+00:00").replace("+00:00", ""))
        except ValueError:
            continue
        if s_dt <= cutoff:
            previous = s
            break
    if previous is None:
        return None

    delta = latest_price - previous["price_dkk"]
    try:
        prev_dt = datetime.fromisoformat(
            previous["observed_at"].replace("Z", "+00:00").replace("+00:00", "")
        )
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


def _detect_status_anomaly_from_list(snapshots: list) -> Optional[dict]:
    """Status-anomali fra præ-fetchet liste. Samme logik som db.detect_status_anomaly()."""
    if len(snapshots) < 2:
        return None

    def _categorize(s):
        sl = (s or "").strip().lower()
        if sl in ("åben", "ledig", "garanteret"):
            return "selling"
        if sl == "få pladser":
            return "late_selling"
        if sl in ("på forespørgsel", "afventer pris"):
            return "withdrawn"
        if sl == "udsolgt":
            return "sold_out"
        return "unknown"

    latest = snapshots[0]
    latest_cat = _categorize(latest["availability_status"])

    previous = None
    for s in snapshots[1:]:
        if _categorize(s["availability_status"]) != latest_cat:
            previous = s
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
    if prev_cat in ("selling", "late_selling") and latest_cat == "withdrawn":
        return {
            "anomaly_type": "withdrawn",
            "severity": "high",
            "label": f"Trukket fra salg (var '{previous['availability_status']}')",
            **base,
        }
    if prev_cat == "selling" and latest_cat == "sold_out":
        return {
            "anomaly_type": "fast_sellout",
            "severity": "medium",
            "label": "Hurtigt udsolgt (sprang 'Få pladser' over)",
            **base,
        }
    return None


def _departure_with_delta(
    conn: Connection,
    operator: str,
    tour_slug: str,
    d: Row,
    tour_run: str | None = None,
    fields: tuple[str, ...] = (),
    lookback_days: int = 7,
) -> dict:
    """Build departure dict with priceDelta info, archive-flag, and status-anomaly.

    Always emits: startDate, priceDkk, status, lastSeenRun, isArchived.
    Conditionally emits extra fields based on `fields` tuple:
      - "endDate" → end_date
      - "departureCode" → departure_code
      - "rejseleder" → rejseleder_name
      - "flightOrigin" → flight_origin

    priceDelta block (kun hvis en prior observation ≥ lookback_days ældre eksisterer):
      - priceDelta: int (current - previous)
      - priceDeltaPrevious: int (previous price)
      - priceDeltaObservedAt: ISO timestamp af tidligere obs
      - priceDeltaDaysAgo: int

    isArchived: true hvis denne afgang ikke længere ses på operatørens side
      (last_seen_run != tour_run). Bruges i UI til at vise historik.

    statusAnomaly: dict eller null. Sat hvis status-historik viser interessant
      overgang (withdrawn, fast_sellout). Se detect_status_anomaly() i db.py.
    """
    out = {
        "startDate": d["start_date"],
        "priceDkk": d["price_dkk"],
        "status": d["availability_status"],
        "lastSeenRun": d["last_seen_run"],
        "isArchived": (tour_run is not None and d["last_seen_run"] != tour_run),
    }
    if "endDate" in fields:
        out["endDate"] = d["end_date"]
    if "departureCode" in fields:
        out["departureCode"] = d["departure_code"]
    if "rejseleder" in fields:
        out["rejseleder"] = d["rejseleder_name"]
    if "flightOrigin" in fields:
        out["flightOrigin"] = d["flight_origin"]

    # Pris-ændring + status-anomali: brug præ-fetchet snapshots-cache
    # (sat af export() før loop). Falder tilbage til DB-query hvis ikke sat.
    key = (operator, tour_slug, d["start_date"])
    snapshots = _SNAPSHOTS_CACHE.get(key)

    if snapshots is not None:
        # Hurtig path: in-memory beregning
        try:
            change = _get_price_change_from_list(snapshots, lookback_days=lookback_days)
            if change:
                out["priceDelta"] = change["delta"]
                out["priceDeltaPrevious"] = change["previous_price"]
                out["priceDeltaObservedAt"] = change["previous_observed_at"]
                out["priceDeltaDaysAgo"] = change["days_ago"]
        except Exception:  # noqa: BLE001
            pass
        try:
            anomaly = _detect_status_anomaly_from_list(snapshots)
            if anomaly:
                out["statusAnomaly"] = anomaly
        except Exception:  # noqa: BLE001
            pass
    else:
        # Fallback path: original DB-query opførsel (langsom, kun ved CLI scrape)
        try:
            change = get_price_change(
                conn,
                operator=operator,
                tour_slug=tour_slug,
                start_date=d["start_date"],
                lookback_days=lookback_days,
            )
            if change:
                out["priceDelta"] = change["delta"]
                out["priceDeltaPrevious"] = change["previous_price"]
                out["priceDeltaObservedAt"] = change["previous_observed_at"]
                out["priceDeltaDaysAgo"] = change["days_ago"]
        except Exception:  # noqa: BLE001
            pass
        try:
            anomaly = detect_status_anomaly(
                conn,
                operator=operator,
                tour_slug=tour_slug,
                start_date=d["start_date"],
            )
            if anomaly:
                out["statusAnomaly"] = anomaly
        except Exception:  # noqa: BLE001
            pass

    return out


def _avg_price(prices: list[Optional[int]]) -> Optional[int]:
    valid = [p for p in prices if p]
    return int(sum(valid) / len(valid)) if valid else None


def _min_price(prices: list[Optional[int]]) -> Optional[int]:
    valid = [p for p in prices if p]
    return min(valid) if valid else None


def _select_anchor(competitor_rows: list[Row], tour_code: str) -> Optional[Row]:
    """Pick the best Tier 1/2 competitor as anchor for this tour-code."""
    by_op = {r["operator"]: r for r in competitor_rows}
    preferred = ANCHOR_PREF_BY_TOUR.get(tour_code, [])
    for op in preferred:
        if op in by_op:
            return by_op[op]
    return next(iter(competitor_rows), None)


def _compute_flags(topas_deps: list, anchor_deps: list) -> dict:
    """Derive flags from observed data."""
    flags = {
        "priceChange": False,
        "slopeMismatch": False,
        "competitorSellout": False,
    }
    if anchor_deps:
        flags["competitorSellout"] = any(d["availability_status"] == "Udsolgt" for d in anchor_deps)
        topas_priced = [(d["start_date"], d["price_dkk"]) for d in topas_deps if d["price_dkk"]]
        anchor_priced = [(d["start_date"], d["price_dkk"]) for d in anchor_deps if d["price_dkk"]]
        if len(topas_priced) >= 2 and len(anchor_priced) >= 2:
            t_slope = topas_priced[-1][1] - topas_priced[0][1]
            a_slope = anchor_priced[-1][1] - anchor_priced[0][1]
            min_topas = min(p for _, p in topas_priced)
            min_anchor = min(p for _, p in anchor_priced)
            if (t_slope * a_slope < 0
                    and (abs(t_slope) / min_topas > 0.05 or abs(a_slope) / min_anchor > 0.05)):
                flags["slopeMismatch"] = True
    return flags


def _run_started(conn: Connection, run_id: str) -> Optional[str]:
    row = conn.execute("SELECT started_at FROM scraper_runs WHERE run_id = ?", (run_id,)).fetchone()
    return row["started_at"] if row else None
