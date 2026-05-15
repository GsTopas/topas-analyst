"""
Competitor-screening pipeline. Python-version af n8n's "Competitor Tour Search"
workflow (ID: x6ETjZx9qqYQ0UZJ).

Same business logic, written in Python so it runs in-process with the rest of
the app:
  1. For each competitor domain: Firecrawl Search → top 10 URLs
  2. Merge with sitemap-hints (max 25 URLs per domain)
  3. Firecrawl Scrape each URL (parallelt)
  4. Claude Sonnet 4.6 classifier — verbatim prompt fra n8n (AND/OR boolean
     keyword filter, hard exclusions, hasGuide, hasFixedDepartures, etc.)
  5. Apply ±15% duration tolerance (downgrade confidence hvis udenfor range)
  6. INSERT rows i n8n_candidates-tabellen i Supabase

Kald `screen_competitors(...)` fra UI (pages/2) eller fra CLI for at trigge
en screening. Returnerer (candidates_count, dict med statistik).
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import requests
from anthropic import Anthropic

from . import catalog_db


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Prompt — VERBATIM fra n8n workflow node "Analyze Competitor" (versionId
# 48df7e32). Hvis du opdaterer prompten, opdater også versionId-noten.
# ---------------------------------------------------------------------------
CLASSIFIER_PROMPT = """You are doing DISCOVERY for Topas Travel — a Danish operator running FIXED-DEPARTURE group tours with a Danish-speaking tour leader. Your job is to identify which competitor URLs are real fixed-departure guided group tours.

You see FULL page content (not snippets). Look at the entire content carefully — INCLUDING the bottom of the page where booking sections often sit.

A MATCH means ALL THREE are true:
1. Has a Danish-speaking tour leader / "rejseleder" / "turleder" / "vandreleder" / "cykelleder"
2. Has fixed published departures (specific calendar dates with prices in a booking widget, "Afgange"-section, departure list, "Datoer og priser"-link, etc.)
3. Goes to the requested country (and matches the keyword/region filter if specified)

If the input says "(No pages scraped for this competitor — Firecrawl Search returned 0 URLs.)" — return matches=[] with noMatchReason="Search returned 0 URLs for this competitor."

==========================================================================
KEYWORD/REGION FILTER — supports AND/OR boolean operators
==========================================================================

The filter can be a single keyword, an OR-list, an AND-list, or a combination.
PRECEDENCE: OR binds tighter than AND.

To parse, split first on " AND " into required GROUPS, then split each group
on " OR " into acceptable ALTERNATIVES.

Examples (assume country="Italien"):

 1. "Apulien"
    → 1 group: [["Apulien"]]
    → Tour MUST be about Apulien.

 2. "Cykling OR Cykelferie"
    → 1 group: [["Cykling","Cykelferie"]]   (synonyms within ONE group)
    → Tour matches if it has cykling OR cykelferie content.

 3. "Apulien AND Cykling"
    → 2 groups: [["Apulien"], ["Cykling"]]
    → Tour MUST be about Apulien AND must be cykling.

 4. "Apulien AND Cykling OR Cykelferie"
    → 2 groups: [["Apulien"], ["Cykling","Cykelferie"]]
    → Tour MUST be about Apulien AND must be cykling-or-cykelferie.

 5. "Cykling OR Cykelferie AND Apulien OR Sicilien"
    → 2 groups: [["Cykling","Cykelferie"], ["Apulien","Sicilien"]]
    → cykling-or-cykelferie tour AND (apulien-or-sicilien) destination.

RULE FOR "high" CONFIDENCE: ALL AND-groups must match (each via at least one
OR-alternative). Plus country must match.

RULE FOR "medium": Country matches but at least ONE AND-group has no match.

Recognize activity stems by Danish: "cykel", "vandr", "trek", "hike",
"kultur", "rundrejse", "ski". Place names are typically capitalized.
If keyword/region is empty: just match on country + tour format.

==========================================================================

NOTE ON DURATION: Do NOT factor duration into your confidence rating.
Downstream code applies a strict duration tolerance ±15% (min ±2 days)
deterministically. Just classify based on country, keyword/region, and
match quality. Set durationDays accurately so downstream can apply the rule.

==========================================================================

HARD EXCLUSIONS — never matches:
- Self-drive tours, roadtrips, rental-car packages
- Individual or private tours, custom-built itineraries
- "Kontakt os for pris", "På forespørgsel", "Bestil tilbud", "Indhent tilbud"
- Hotel-only or flight-only bookings
- Generic cruises without a Danish-speaking guide
- Destination overview / inspiration / country-overview pages
- Tour leader profile pages

For EACH match, verify and report TWO booleans honestly. CRITICAL: each can be true, false, OR null (unknown).

- hasGuide:
  • true — Danish-speaking tour leader explicitly mentioned for THIS tour
  • false — page explicitly states no Danish guide
  • null — couldn't determine from scraped content

- hasFixedDepartures:
  • true — you found ACTUAL departure dates with prices in the content
  • false — page explicitly states no departures available
  • null — couldn't find specific dates BUT there's a clear booking-section
    anchor/tab/heading. NEVER set to false in this case — set null.

  Use null GENEROUSLY. False positives ("Nej" when it's actually yes) are
  much worse than nulls.

For each match, ALSO classify:
- tourCategory: "vandre" | "cykel" | "kultur" | "kombineret" | "andet"

For each match, also extract:
- tourName, tourUrl, durationDays (0 if unknown), matchConfidence, notes

Return up to 8 distinct guided group tours per competitor.
Be GENEROUS with "high"-confidence — if a tour clearly matches country +
keyword/region, mark it high. Downstream system passes ALL high-confidence
matches uncapped (after duration-tolerance is applied in code).

If no real matches, return matches=[] and explain in noMatchReason.

OUTPUT FORMAT — every field is required, but hasGuide/hasFixedDepartures CAN be null:
- All string fields use empty string "" if unknown
- durationDays = 0 if unknown
- hasGuide and hasFixedDepartures are true | false | null
- matchConfidence is "high" | "medium" | "low" exactly
- tourCategory is "vandre" | "cykel" | "kultur" | "kombineret" | "andet" exactly

OUTPUT JSON SHAPE (verbatim):
{
  "matches": [
    {
      "tourName": "...",
      "tourUrl": "...",
      "durationDays": 10,
      "matchConfidence": "high",
      "hasGuide": true,
      "hasFixedDepartures": true,
      "tourCategory": "cykel",
      "notes": "..."
    }
  ],
  "noMatchReason": ""
}

Return ONLY the JSON object — no prose, no markdown fences.

SEARCH INPUT:
{search_blob}
"""


SEARCH_KEYWORDS = (
    "rejseleder OR turleder OR fællesrejse OR fællesrejser OR rundrejse OR "
    "vandreferie OR vandretur OR vandreture OR vandring OR cykeltur OR "
    "cykelferie OR cykling OR cykel OR gruppetur OR aktivferie"
)

CLAUDE_MODEL = "claude-sonnet-4-5"  # n8n bruger sonnet-4-6 — vi bruger seneste sonnet
# Bump til opus-4-7 hvis Gorm vil have endnu bedre kvalitet — koster mere

# Confidence-downgrade tabel (samme som n8n's expand-matches)
_DOWNGRADE = {"high": "medium", "medium": "low", "low": "low"}
_CONFIDENCE_ORDER = {"high": 0, "medium": 1, "low": 2}
_ALLOWED_CATEGORIES = {"vandre", "cykel", "kultur", "kombineret", "andet"}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ScreeningContext:
    competitor_domain: str
    search_country: str
    search_region: str
    topas_tour_code: str
    topas_duration_days: Optional[int]
    sitemap_hints: list[str] = field(default_factory=list)


@dataclass
class CandidateRow:
    competitor_domain: str
    search_country: str
    search_region: str
    topas_tour_code: str
    has_match: bool
    tour_name: str
    tour_url: str
    duration_days: Optional[int]
    match_confidence: str
    has_guide: Optional[bool]
    has_fixed_departures: Optional[bool]
    tour_category: str
    notes: str
    searched_at: str


ProgressCallback = Callable[[str], None]


# ---------------------------------------------------------------------------
# Firecrawl search + scrape (raw HTTP — matcher n8n-workflowets kald)
# ---------------------------------------------------------------------------

def _firecrawl_search(query: str, limit: int, api_key: str) -> list[dict]:
    """Firecrawl /v1/search. Returnerer liste af {url, title, description}."""
    r = requests.post(
        "https://api.firecrawl.dev/v1/search",
        headers={"Authorization": f"Bearer {api_key}"},
        json={"query": query, "limit": limit},
        timeout=30,
    )
    if r.status_code >= 400:
        log.warning("Firecrawl search fejlede for %r: HTTP %d — %s", query, r.status_code, r.text[:200])
        return []
    data = r.json().get("data")
    if not isinstance(data, list):
        log.warning("Firecrawl search: data ikke liste (type=%s) for %r", type(data).__name__, query)
        return []
    return [r for r in data if isinstance(r, dict) and r.get("url")]


def _firecrawl_scrape_markdown(url: str, api_key: str) -> Optional[str]:
    """Firecrawl /v1/scrape, returnerer markdown eller None ved fejl."""
    try:
        r = requests.post(
            "https://api.firecrawl.dev/v1/scrape",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "url": url,
                "formats": ["markdown"],
                "onlyMainContent": False,
                "waitFor": 5000,
                "timeout": 30000,
            },
            timeout=60,
        )
        if r.status_code >= 400:
            log.warning("Firecrawl scrape fejlede for %s: HTTP %d", url, r.status_code)
            return None
        payload = r.json().get("data") or {}
        return (payload.get("markdown") or "")[:50000]
    except Exception:
        log.exception("Firecrawl scrape exception for %s", url)
        return None


# ---------------------------------------------------------------------------
# Classifier (Anthropic)
# ---------------------------------------------------------------------------

def _build_search_blob(ctx: ScreeningContext, pages: list[dict]) -> str:
    """Bygger den tekst-blob Claude analyserer. Matcher n8n's Format Analysis Input."""
    blob = f"Competitor domain: {ctx.competitor_domain}\n"
    blob += f"Searching for: guided group tour to {ctx.search_country}"
    if ctx.search_region:
        blob += f" — keyword/region filter: {ctx.search_region}"
    blob += "\n\n"
    if not pages:
        blob += "(No pages scraped for this competitor — Firecrawl Search returned 0 URLs.)"
        return blob
    for i, p in enumerate(pages, 1):
        blob += f"--- Page {i} ---\n"
        blob += f"URL: {p.get('url', '')}\n"
        blob += f"Title: {p.get('title', '')}\n"
        if p.get("description"):
            blob += f"Description: {p['description']}\n"
        blob += f"\nContent:\n{p.get('markdown') or '(empty)'}\n\n"
    return blob


def _classify(search_blob: str, anthropic_client: Anthropic) -> dict:
    """Kalder Claude og parser strukturen JSON ud. Returnerer dict med
    {matches: [...], noMatchReason: ...}. Returnerer tom struktur ved fejl."""
    prompt = CLASSIFIER_PROMPT.replace("{search_blob}", search_blob)
    try:
        resp = anthropic_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(b.text for b in resp.content if hasattr(b, "text"))
    except Exception:
        log.exception("Claude classifier API-kald fejlede")
        return {"matches": [], "noMatchReason": "Classifier API error"}

    # Strip evt. markdown fences + find JSON-objekt
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text)
    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        log.warning("Claude returnerede ingen JSON. Output (200 chars): %s", text[:200])
        return {"matches": [], "noMatchReason": "Classifier returned no JSON"}
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        log.exception("Claude JSON parse fejlede. Output (200 chars): %s", text[:200])
        return {"matches": [], "noMatchReason": "Classifier JSON malformed"}


# ---------------------------------------------------------------------------
# Match-normalisering (port af n8n's "Expand Matches"-node)
# ---------------------------------------------------------------------------

def _to_int_or_none(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        n = int(v)
        return n if n != 0 else None
    except (TypeError, ValueError):
        return None


def _as_bool_or_none(v: Any) -> Optional[bool]:
    if v is True or v is False:
        return v
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes", "ja"):
            return True
        if s in ("false", "0", "no", "nej"):
            return False
        if s in ("null", "unknown", "ukendt"):
            return None
    return None


def _norm_category(v: Any) -> str:
    if not v:
        return ""
    s = str(v).strip().lower()
    return s if s in _ALLOWED_CATEGORIES else ""


def _apply_duration_penalty(
    confidence: str, comp_duration: Optional[int], topas_duration: Optional[int]
) -> tuple[str, bool, Optional[int], Optional[int]]:
    """Returnerer (ny_confidence, applied, range_lower, range_upper)."""
    if not topas_duration or not comp_duration:
        return confidence, False, None, None
    tolerance = max(2, topas_duration * 0.15)
    lower = topas_duration - tolerance
    upper = topas_duration + tolerance
    if comp_duration < lower or comp_duration > upper:
        return _DOWNGRADE.get(confidence, confidence), True, int(lower) + 1, int(upper)
    return confidence, False, None, None


def _normalize_matches(
    classifier_output: dict, ctx: ScreeningContext, page_count: int
) -> list[CandidateRow]:
    """Tager classifier-output + kontekst → liste af CandidateRow.

    Anvender duration-penalty + ALLE high + top-3 non-high (samme som n8n).
    """
    raw_matches = classifier_output.get("matches") or []
    no_match_reason = (classifier_output.get("noMatchReason") or "").strip()
    searched_at = datetime.now(timezone.utc).isoformat()

    # Duration-penalty + downgrade
    for m in raw_matches:
        if not isinstance(m, dict):
            continue
        comp_d = _to_int_or_none(m.get("durationDays"))
        new_conf, applied, lo, hi = _apply_duration_penalty(
            (m.get("matchConfidence") or "").strip(), comp_d, ctx.topas_duration_days
        )
        if applied:
            m["matchConfidence"] = new_conf
            suffix = (
                f" [Auto-downgrade: {comp_d}d vs Topas {ctx.topas_duration_days}d "
                f"(range {lo}-{hi})]"
            )
            m["notes"] = ((m.get("notes") or "").strip() + suffix).strip()

    # Sortér + behold ALLE high + top 3 non-high
    raw_matches.sort(
        key=lambda m: _CONFIDENCE_ORDER.get((m or {}).get("matchConfidence", "low"), 99)
    )
    high = [m for m in raw_matches if (m or {}).get("matchConfidence") == "high"]
    non_high = [m for m in raw_matches if (m or {}).get("matchConfidence") != "high"]
    selected = list(high) + list(non_high[:3])

    if not selected:
        final_note = no_match_reason or (
            "Search returned 0 URLs for this competitor." if page_count == 0 else ""
        )
        return [
            CandidateRow(
                competitor_domain=ctx.competitor_domain,
                search_country=ctx.search_country,
                search_region=ctx.search_region,
                topas_tour_code=ctx.topas_tour_code,
                has_match=False,
                tour_name="",
                tour_url="",
                duration_days=None,
                match_confidence="",
                has_guide=None,
                has_fixed_departures=None,
                tour_category="",
                notes=final_note,
                searched_at=searched_at,
            )
        ]

    out: list[CandidateRow] = []
    for m in selected:
        if not isinstance(m, dict):
            continue
        has_guide = _as_bool_or_none(m.get("hasGuide"))
        has_fixed = _as_bool_or_none(m.get("hasFixedDepartures"))
        out.append(
            CandidateRow(
                competitor_domain=ctx.competitor_domain,
                search_country=ctx.search_country,
                search_region=ctx.search_region,
                topas_tour_code=ctx.topas_tour_code,
                has_match=(has_guide is True and has_fixed is True),
                tour_name=str(m.get("tourName") or ""),
                tour_url=str(m.get("tourUrl") or ""),
                duration_days=_to_int_or_none(m.get("durationDays")),
                match_confidence=str(m.get("matchConfidence") or ""),
                has_guide=has_guide,
                has_fixed_departures=has_fixed,
                tour_category=_norm_category(m.get("tourCategory")),
                notes=str(m.get("notes") or ""),
                searched_at=searched_at,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Per-competitor pipeline
# ---------------------------------------------------------------------------

def _normalize_domain(raw: str) -> str:
    """Strip https://, trailing slash, whitespace."""
    return raw.strip().replace("https://", "").replace("http://", "").rstrip("/")


def _screen_one_competitor(
    ctx: ScreeningContext,
    firecrawl_api_key: str,
    anthropic_client: Anthropic,
    emit: ProgressCallback,
    scrape_workers: int,
) -> list[CandidateRow]:
    """Kører hele pipelinen for ÉN konkurrent: search → scrape → classify."""
    domain = ctx.competitor_domain
    emit(f"[{domain}] Søger Firecrawl...")

    query = f"site:{domain} ({SEARCH_KEYWORDS}) {ctx.search_country} {ctx.search_region}".strip()
    search_results = _firecrawl_search(query, limit=10, api_key=firecrawl_api_key)
    emit(f"[{domain}] ✓ {len(search_results)} URLs fra search")

    # Merge med sitemap-hints, dedupliker, cap til 25
    candidate_urls: list[tuple[str, str, str, str]] = []  # (url, title, description, source)
    seen: set[str] = set()
    for r in search_results:
        url = r.get("url", "")
        if not url or url in seen:
            continue
        seen.add(url)
        candidate_urls.append((url, r.get("title", ""), r.get("description", ""), "search"))
    for url in ctx.sitemap_hints:
        if not url or url in seen:
            continue
        seen.add(url)
        candidate_urls.append((url, "", "(from sitemap)", "sitemap"))
    candidate_urls = candidate_urls[:25]

    if not candidate_urls:
        emit(f"[{domain}] Ingen URLs at scrape — springer scrape over")
        rows = _normalize_matches({"matches": [], "noMatchReason": ""}, ctx, page_count=0)
        return rows

    # Parallel scrape af de fundne URLs
    emit(f"[{domain}] Scraping {len(candidate_urls)} URLs ({scrape_workers} parallel)...")
    pages: list[dict] = []
    pages_lock = threading.Lock()

    def _scrape_one(item: tuple[str, str, str, str]) -> None:
        url, title, description, source = item
        md = _firecrawl_scrape_markdown(url, firecrawl_api_key)
        with pages_lock:
            pages.append({
                "url": url,
                "title": title,
                "description": description,
                "source": source,
                "markdown": md or "",
            })

    with ThreadPoolExecutor(max_workers=scrape_workers) as ex:
        list(ex.map(_scrape_one, candidate_urls))

    emit(f"[{domain}] ✓ Scraping færdig · klassificerer med Claude...")
    search_blob = _build_search_blob(ctx, pages)
    classifier_output = _classify(search_blob, anthropic_client)
    rows = _normalize_matches(classifier_output, ctx, page_count=len(pages))

    n_high = sum(1 for r in rows if r.match_confidence == "high")
    n_total = sum(1 for r in rows if r.tour_name)  # skip placeholder empty row
    emit(f"[{domain}] ✓ {n_high} high · {n_total} total kandidater")
    return rows


# ---------------------------------------------------------------------------
# DB-persistens
# ---------------------------------------------------------------------------

def _save_rows_to_supabase(rows: list[CandidateRow]) -> int:
    """INSERT batch af kandidater i n8n_candidates-tabellen. Returnerer
    antal indsatte rækker."""
    if not rows:
        return 0
    conn = catalog_db.connect()
    raw_cur = conn._conn.cursor()
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        from psycopg2.extras import execute_values  # local import — psycopg2 kun via _pg_conn
        payload = [
            (
                r.competitor_domain,
                r.topas_tour_code,
                r.search_country,
                r.search_region,
                1 if r.has_match else 0,
                r.tour_name,
                r.tour_url,
                "",  # next_departure — fyldes ud ved scrape, ikke screening
                "",  # price — samme
                r.tour_category,
                r.duration_days,
                r.match_confidence,
                r.notes,
                r.searched_at,
                now_iso,        # n8n_created_at (matcher n8n's timestamp)
                now_iso,        # imported_at
                "[]",           # departures_json
                None if r.has_guide is None else (1 if r.has_guide else 0),
                None if r.has_fixed_departures is None else (1 if r.has_fixed_departures else 0),
            )
            for r in rows
        ]
        execute_values(
            raw_cur,
            """
            INSERT INTO n8n_candidates (
                competitor_domain, topas_tour_code, search_country, search_region,
                has_match, tour_name, tour_url, next_departure, price, tour_category,
                duration_days, match_confidence, notes, searched_at, n8n_created_at,
                imported_at, departures_json, has_guide, has_fixed_departures
            ) VALUES %s
            """,
            payload,
        )
        conn.commit()
        return len(payload)
    finally:
        raw_cur.close()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def screen_competitors(
    competitor_domains: list[str],
    country: str,
    region: str = "",
    topas_tour_code: str = "",
    topas_duration_days: Optional[int] = None,
    sitemap_hints: Optional[dict[str, list[str]]] = None,
    on_progress: Optional[ProgressCallback] = None,
    competitor_workers: int = 5,
    scrape_workers_per_competitor: int = 4,
) -> tuple[int, dict]:
    """Kør discovery-pipelinen for en liste af konkurrent-domæner.

    Args:
        competitor_domains: liste af "albatros.dk" / "https://albatros.dk/"
        country: target country (fx "Italien")
        region: AND/OR keyword filter (fx "Apulien AND Cykling")
        topas_tour_code: tour-kode (fx "ITTO")
        topas_duration_days: tour-varighed for duration tolerance check
        sitemap_hints: dict af domain → list af extra URLs (fx fra hver
                       operators sitemap.xml)
        on_progress: callback for live status-besked
        competitor_workers: hvor mange konkurrenter screenes samtidigt
        scrape_workers_per_competitor: scrape-parallelisme per konkurrent

    Returns:
        (rows_inserted, stats) hvor stats = {domains, total_candidates,
        high_confidence, errors}
    """
    def emit(msg: str) -> None:
        if on_progress:
            on_progress(msg)
        else:
            log.info(msg)

    firecrawl_api_key = os.environ.get("FIRECRAWL_API_KEY")
    if not firecrawl_api_key:
        raise RuntimeError("FIRECRAWL_API_KEY ikke sat")

    anthropic_client = Anthropic()  # bruger ANTHROPIC_API_KEY fra env

    # Build kontekster
    contexts: list[ScreeningContext] = []
    seen_domains: set[str] = set()
    hints = sitemap_hints or {}
    for raw in competitor_domains:
        d = _normalize_domain(raw)
        if not d or d in seen_domains:
            continue
        seen_domains.add(d)
        contexts.append(
            ScreeningContext(
                competitor_domain=d,
                search_country=country.strip(),
                search_region=region.strip(),
                topas_tour_code=topas_tour_code.strip(),
                topas_duration_days=topas_duration_days,
                sitemap_hints=list(hints.get(d, [])),
            )
        )

    if not contexts:
        raise ValueError("Ingen gyldige konkurrent-domæner")

    emit(
        f"Starter screening · {len(contexts)} konkurrenter · "
        f"{competitor_workers} parallel · {country!r} {region!r}"
    )

    # Hvis vi kører i Streamlit-context, så får worker-threads ikke automatisk
    # session-context (Streamlit's ScriptRunContext er thread-local). Det
    # gør at on_progress-callbacks der kalder st.status/st.write crasher med
    # NoSessionContext. Fix: propagér main-thread's context til hver worker.
    streamlit_ctx = None
    try:
        from streamlit.runtime.scriptrunner import (  # noqa: PLC0415
            get_script_run_ctx,
            add_script_run_ctx,
        )
        streamlit_ctx = get_script_run_ctx()
    except Exception:
        # Streamlit ikke installeret eller vi kører fra CLI — fint
        pass

    all_rows: list[CandidateRow] = []
    errors = 0
    rows_lock = threading.Lock()

    def _process(ctx: ScreeningContext) -> None:
        nonlocal errors
        # Tilføj Streamlit-context til denne worker så emit() (som kalder
        # st.status.update i Streamlit-kontekst) ikke crasher.
        if streamlit_ctx is not None:
            try:
                add_script_run_ctx(threading.current_thread(), streamlit_ctx)
            except Exception:
                pass  # ikke kritisk — emit() vil bare ikke opdatere UI
        try:
            rows = _screen_one_competitor(
                ctx, firecrawl_api_key, anthropic_client, emit,
                scrape_workers=scrape_workers_per_competitor,
            )
            with rows_lock:
                all_rows.extend(rows)
        except Exception as e:
            log.exception("Screening fejlede for %s", ctx.competitor_domain)
            emit(f"[{ctx.competitor_domain}] ✗ {e}")
            errors += 1

    with ThreadPoolExecutor(max_workers=competitor_workers) as ex:
        for fut in as_completed([ex.submit(_process, c) for c in contexts]):
            fut.result()

    emit(f"✓ Klassificering færdig · gemmer {len(all_rows)} rækker i Supabase...")
    inserted = _save_rows_to_supabase(all_rows)
    emit(f"✓ {inserted} kandidat-rækker indsat")

    stats = {
        "domains": len(contexts),
        "total_candidates": len(all_rows),
        "high_confidence": sum(1 for r in all_rows if r.match_confidence == "high"),
        "errors": errors,
    }
    return inserted, stats
