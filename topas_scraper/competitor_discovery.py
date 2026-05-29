"""
Competitor Discovery — find tours competitors have that Topas does NOT.

Flow for ONE operator (e.g. gjoa.dk):
  1. discover_operator_tours() → list of tour-URLs (sitemap + Firecrawl /map)
  2. For each URL: scrape with Firecrawl JSON-extract → tour metadata
  3. Filter to ICP:
     - has_guide = True (Danish tour leader)
     - has_fixed_departures = True
     - activity ∈ {Vandre, Trek, Cykling, Sejlads-vandring, Højrute, ...}
     - duration_days ∈ [4, 25]
  4. Gap-analyse mod topas_catalog:
     - For (country, activity, duration-band) — er der en Topas-tur i samme band?
     - Hvis ikke → gap
  5. Score gap-ture by importance:
     - Antal afgange næste 12 mdr (mere = højere)
     - Country-strategy bonus (Topas-fokus-lande)
     - Negative bonus hvis lignende tur blev afvist tidligere (review_decisions)

Output: ranked liste af GapResult med ekstra metadata til UI.

Genbruger:
  - topas_scraper.discovery — URL discovery
  - topas_scraper.client.FirecrawlClient — scraping
  - topas_scraper.extraction_schema.EXTRACTION_SCHEMA — JSON-extract schema
  - topas_scraper._pg_conn — Supabase access
"""
from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from .client import FirecrawlClient
from .discovery import discover_operator_tours, DiscoveryResult

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ICP-konfiguration
# ---------------------------------------------------------------------------

# Aktiviteter Topas dækker. Konkurrent-tur skal være i denne mængde for at
# regnes som ICP-relevant.
ICP_ACTIVITIES: set[str] = {
    "Vandring",
    "Trekking",
    "Cykling",
    "Sejlads og vandring",
    "Højrute",
    "Højvandring",
    "Bjergvandring",
    "Sejlads og cykling",
}

ICP_DURATION_RANGE: tuple[int, int] = (4, 25)

# Country-strategy bonus — Topas-fokus-lande får ekstra score
COUNTRY_PRIORITY: dict[str, float] = {
    "Grønland": 1.5,
    "Italien": 1.3,
    "Spanien": 1.3,
    "Nepal": 1.2,
    "Vietnam": 1.2,
    "Portugal": 1.2,
    "Frankrig": 1.2,
    "Kroatien": 1.2,
}

# Duration-bands til gap-analyse — to ture i samme band konkurrerer om kunden
DURATION_BANDS: list[tuple[int, int]] = [
    (4, 6),
    (6, 9),
    (9, 12),
    (12, 16),
    (16, 22),
    (22, 30),
]


def _band_for_duration(days: int) -> tuple[int, int]:
    """Find varigheds-bånd for en tur. Returns (low, high)."""
    for low, high in DURATION_BANDS:
        if low <= days < high:
            return (low, high)
    return (22, 30)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CompetitorTour:
    """Klassificeret konkurrent-tur klar til gap-analyse."""
    operator: str
    url: str
    slug: str
    tour_name: str
    country: Optional[str]
    activity: Optional[str]
    duration_days: Optional[int]
    has_guide: bool
    has_fixed_departures: bool
    next_departure: Optional[str]
    departure_count_next_12mo: int
    from_price_dkk: Optional[int]
    icp_match: bool
    classifier_notes: str = ""


@dataclass
class GapResult:
    """Tur konkurrenten har som Topas IKKE har. Sorteret efter score."""
    tour: CompetitorTour
    gap_reason: str
    score: float
    rejected_similar_count: int = 0
    rejected_similar_reasons: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Topas baseline
# ---------------------------------------------------------------------------

def _activity_keywords(activity: str) -> set[str]:
    """Map en aktivitet til match-keywords for fuzzy matching."""
    activity_lc = (activity or "").lower()
    hiking = {"vandr", "trek", "højrute", "højvandr", "bjergvandr"}
    biking = {"cykl", "cykel", "mountain"}
    sailing = {"sejlads"}

    out: set[str] = set()
    if any(k in activity_lc for k in hiking):
        out.add("vandring")
    if any(k in activity_lc for k in biking):
        out.add("cykling")
    if any(k in activity_lc for k in sailing):
        out.add("sejlads")
    return out


def _build_topas_baseline(conn) -> set[tuple[str, str, tuple[int, int]]]:
    """Returnerer sæt af (country, activity-bucket, duration-band) Topas dækker."""
    rows = conn.execute("""
        SELECT tour_name, country, duration_days
        FROM topas_catalog
        WHERE (audience_segment IS NULL OR audience_segment != 'Udgået')
          AND country IS NOT NULL
          AND duration_days IS NOT NULL
    """).fetchall()

    coverage: set[tuple[str, str, tuple[int, int]]] = set()
    for r in rows:
        d = dict(r)
        name = (d["tour_name"] or "").lower()
        country = d["country"]
        days = int(d["duration_days"])

        activities: set[str] = set()
        if any(k in name for k in ("vandr", "trek", "højrute")):
            activities.add("vandring")
        if any(k in name for k in ("cykl", "mountain")):
            activities.add("cykling")
        if "sejlads" in name:
            activities.add("sejlads")
        if not activities:
            # Fallback: hvis vi ikke kan udlede, antag vandring
            activities.add("vandring")

        band = _band_for_duration(days)
        for act in activities:
            coverage.add((country, act, band))

    return coverage


# ---------------------------------------------------------------------------
# Lœrings-base — review_decisions
# ---------------------------------------------------------------------------

def _build_rejection_patterns(conn) -> list[dict]:
    """Hent afviste kandidater + deres reason fra review_decisions."""
    rows = conn.execute("""
        SELECT rd.reason, n8c.tour_name, n8c.tour_category, n8c.search_country
        FROM review_decisions rd
        JOIN n8n_candidates n8c ON n8c.n8n_row_id = rd.target_id
        WHERE rd.action = 'reject'
          AND rd.target_kind = 'n8n_candidate'
          AND rd.reason IS NOT NULL
    """).fetchall()
    return [dict(r) for r in rows]


def _count_rejection_similarity(
    tour: CompetitorTour,
    rejections: list[dict],
) -> tuple[int, list[str]]:
    """Tæl hvor mange afviste kandidater der ligner denne tur."""
    if not tour.country or not tour.activity:
        return 0, []

    tour_keywords = _activity_keywords(tour.activity)
    if not tour_keywords:
        return 0, []

    matches: list[str] = []
    for r in rejections:
        rej_country = (r.get("search_country") or "").strip()
        rej_category = (r.get("tour_category") or "").lower()

        if rej_country != tour.country:
            continue
        rej_keywords = _activity_keywords(rej_category)
        if rej_keywords & tour_keywords:
            reason = r.get("reason") or "Ukendt"
            matches.append(reason)

    return len(matches), matches[:5]


# ---------------------------------------------------------------------------
# Gap detection + scoring
# ---------------------------------------------------------------------------

def _detect_gap(tour: CompetitorTour, topas_coverage: set) -> Optional[str]:
    """Returnerer gap-grund hvis turen IKKE er dækket af Topas, ellers None."""
    if not tour.country or not tour.duration_days or not tour.activity:
        return None

    band = _band_for_duration(tour.duration_days)
    activities = _activity_keywords(tour.activity)

    for act in activities:
        if (tour.country, act, band) in topas_coverage:
            return None

    activities_str = "/".join(sorted(activities)) or tour.activity
    return f"Topas har ingen {activities_str}-tur i {tour.country} på {band[0]}-{band[1]} dage"


def _score(tour: CompetitorTour, rejected_count: int) -> float:
    """Score-formel for prioritering af gap-ture."""
    base = min(tour.departure_count_next_12mo, 12) * 0.83
    country_mult = COUNTRY_PRIORITY.get(tour.country or "", 1.0)
    rejection_malus = rejected_count * 1.0
    return max(0.0, base * country_mult - rejection_malus)


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------

def run_discovery(
    operator: str,
    homepage_url: str,
    sitemap_url: Optional[str] = None,
    progress_callback: Optional[Callable[[str], None]] = None,
    max_urls: int = 50,
    parallelism: int = 8,
) -> dict:
    """Kør hele discovery-flowet for én konkurrent.

    Returns dict med urls_discovered, tours_classified, icp_passing, gaps, stats.
    """
    def emit(msg: str) -> None:
        log.info(msg)
        if progress_callback:
            try:
                progress_callback(msg)
            except Exception:
                pass

    emit(f"Discovery: {operator} — start")

    fc = FirecrawlClient()
    emit("1/5: Henter tour-URLs (sitemap + Firecrawl-fallback)")
    discovery_result: DiscoveryResult = discover_operator_tours(
        operator=operator,
        homepage_url=homepage_url,
        sitemap_url=sitemap_url,
        firecrawl_client=fc,
    )
    urls_to_scrape = discovery_result.tours_found[:max_urls]
    emit(f"   Fundet {len(discovery_result.tours_found)} URLs via "
         f"{discovery_result.method_used} (scraper top {len(urls_to_scrape)})")

    if not urls_to_scrape:
        return {
            "urls_discovered": 0,
            "tours_classified": 0,
            "icp_passing": 0,
            "gaps": [],
            "stats": {
                "method": discovery_result.method_used,
                "errors": ["Ingen tour-URLs fundet"],
            },
        }

    emit(f"2/5: Scraper + klassificerer {len(urls_to_scrape)} ture "
         f"(parallel x {parallelism})")
    tours: list[CompetitorTour] = []
    errors: list[str] = []

    def _classify_one(url_slug: tuple[str, str]) -> Optional[CompetitorTour]:
        url, slug = url_slug
        try:
            return _scrape_and_classify(fc, operator, url, slug)
        except Exception as exc:
            errors.append(f"{url}: {type(exc).__name__}: {exc}")
            return None

    with ThreadPoolExecutor(max_workers=parallelism) as ex:
        futures = {ex.submit(_classify_one, us): us for us in urls_to_scrape}
        completed = 0
        for fut in as_completed(futures):
            tour = fut.result()
            completed += 1
            if completed % 5 == 0 or completed == len(urls_to_scrape):
                emit(f"   {completed}/{len(urls_to_scrape)} færdig")
            if tour:
                tours.append(tour)

    emit(f"   Klassificeret {len(tours)} ture ({len(errors)} fejl)")

    emit("3/5: Filtrerer på ICP (group + fixed-dates + guided + hiking/biking)")
    icp_tours = [t for t in tours if t.icp_match]
    emit(f"   {len(icp_tours)}/{len(tours)} passerer ICP-filter")

    emit("4/5: Gap-analyse mod Topas-katalog")
    from ._pg_conn import connect as pg_connect
    conn = pg_connect()
    topas_coverage = _build_topas_baseline(conn)
    rejections = _build_rejection_patterns(conn)
    emit(f"   Topas dækker {len(topas_coverage)} (land × aktivitet × varighed)"
         f" · {len(rejections)} historiske afvisninger som lærings-base")

    gaps: list[GapResult] = []
    for tour in icp_tours:
        gap_reason = _detect_gap(tour, topas_coverage)
        if not gap_reason:
            continue
        rej_count, rej_reasons = _count_rejection_similarity(tour, rejections)
        score = _score(tour, rej_count)
        gaps.append(GapResult(
            tour=tour,
            gap_reason=gap_reason,
            score=score,
            rejected_similar_count=rej_count,
            rejected_similar_reasons=rej_reasons,
        ))

    gaps.sort(key=lambda g: g.score, reverse=True)
    emit(f"5/5: Fundet {len(gaps)} gap-ture (rangeret efter score)")

    return {
        "urls_discovered": len(discovery_result.tours_found),
        "tours_classified": len(tours),
        "icp_passing": len(icp_tours),
        "gaps": gaps,
        "stats": {
            "method": discovery_result.method_used,
            "errors": errors[:20],
            "max_urls_capped": len(discovery_result.tours_found) > max_urls,
        },
    }


# ---------------------------------------------------------------------------
# Scrape + classify ÉN tur
# ---------------------------------------------------------------------------

def _scrape_and_classify(
    fc: FirecrawlClient,
    operator: str,
    url: str,
    slug: str,
) -> Optional[CompetitorTour]:
    """Firecrawl-scrape + Claude-classify én tour-URL."""
    from .extraction_schema import EXTRACTION_SCHEMA, EXTRACTION_PROMPT

    try:
        scrape_result = fc.scrape(
            url,
            formats=["markdown", "extract"],
            extract_schema=EXTRACTION_SCHEMA,
            extract_prompt=EXTRACTION_PROMPT,
            only_main_content=True,
            timeout_ms=30_000,
        )
    except Exception as exc:
        log.warning("Scrape failed for %s: %s", url, exc)
        return None

    extracted = getattr(scrape_result, "extracted", None) or {}
    markdown = getattr(scrape_result, "markdown", "") or ""

    classification = _claude_classify_tour(
        operator=operator,
        url=url,
        markdown=markdown[:8_000],
        extracted=extracted,
    )

    departures = extracted.get("departures") or []
    next_12mo = _count_future_departures(departures, months_ahead=12)
    next_dep = _next_departure_iso(departures)

    return CompetitorTour(
        operator=operator,
        url=url,
        slug=slug,
        tour_name=classification.get("tour_name") or "(unknown)",
        country=classification.get("country"),
        activity=classification.get("activity"),
        duration_days=classification.get("duration_days"),
        has_guide=bool(classification.get("has_guide", False)),
        has_fixed_departures=bool(classification.get("has_fixed_departures", False)),
        next_departure=next_dep,
        departure_count_next_12mo=next_12mo,
        from_price_dkk=classification.get("from_price_dkk"),
        icp_match=classification.get("icp_match", False),
        classifier_notes=classification.get("notes", ""),
    )


# ---------------------------------------------------------------------------
# Claude classifier — discovery-fokuseret prompt
# ---------------------------------------------------------------------------

DISCOVERY_CLASSIFIER_PROMPT = """Du klassificerer en konkurrent-tur for Topas Travel.

Topas's ICP: FIXED-DEPARTURE GROUP TOURS med dansk-talende rejseleder, fokus
på VANDRING og CYKLING (samt sejlads-kombi). 6-25 dage typisk.

ANALYSER nedenstående tour-info og udfyld JSON.

URL: {url}
EKSTRAHERET DATA:
{extracted_json}

UDDRAG AF SIDENS MARKDOWN (top):
{markdown_snippet}

Returnér JSON med felterne:
  tour_name: str — turens titel
  country: str — primært land (dansk: "Marokko", "Spanien", "Grønland" etc)
  activity: str — én af "Vandring", "Trekking", "Cykling", "Sejlads og vandring",
                  "Højrute", "Bjergvandring", "Kultur", "Mad og vin", "Yoga",
                  "Self-drive", "Krydstogt", "Forskningsrejse", "Andet"
  duration_days: int — antal dage (null hvis ukendt)
  has_guide: bool — har en dansk-talende rejseleder/turleder/vandreleder
  has_fixed_departures: bool — har fixed publicerede afgangsdatoer + priser
  from_price_dkk: int — frapris i DKK (null hvis ikke vist)
  icp_match: bool — TRUE hvis tour passer Topas ICP (alle krav: has_guide=true,
             has_fixed_departures=true, activity er Vandring/Trekking/Cykling/
             relateret, duration 4-25 dage)
  notes: str — kort begrundelse hvis icp_match=false, eller "OK" hvis true

Returnér KUN JSON, ingen markdown-fences."""


def _claude_classify_tour(
    operator: str,
    url: str,
    markdown: str,
    extracted: dict,
) -> dict:
    """Kald Claude med discovery-prompt for at klassificere én tur."""
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
        import json as _json

        prompt = DISCOVERY_CLASSIFIER_PROMPT.format(
            url=url,
            extracted_json=_json.dumps(extracted, ensure_ascii=False, default=str)[:3000],
            markdown_snippet=markdown[:3000],
        )

        msg = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )

        text = msg.content[0].text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.MULTILINE)
        return _json.loads(text)
    except Exception as exc:
        log.warning("Claude classify failed for %s: %s", url, exc)
        return {
            "tour_name": extracted.get("tour_name") or extracted.get("title") or "?",
            "country": None,
            "activity": None,
            "duration_days": extracted.get("duration_days"),
            "has_guide": False,
            "has_fixed_departures": bool(extracted.get("departures")),
            "from_price_dkk": extracted.get("from_price_dkk"),
            "icp_match": False,
            "notes": f"Claude-kald fejlede: {exc}",
        }


# ---------------------------------------------------------------------------
# Departure-helpers
# ---------------------------------------------------------------------------

def _count_future_departures(departures: list, months_ahead: int = 12) -> int:
    """Tæl hvor mange afgange der ligger inden for X måneder fremad."""
    if not departures:
        return 0
    now = datetime.now(timezone.utc).date()
    cutoff = now + timedelta(days=30 * months_ahead)
    count = 0
    for d in departures:
        if not isinstance(d, dict):
            continue
        start = d.get("start_date") or d.get("startDate")
        if not start:
            continue
        try:
            dt = datetime.fromisoformat(str(start)[:10]).date()
        except ValueError:
            continue
        if now <= dt <= cutoff:
            count += 1
    return count


def _next_departure_iso(departures: list) -> Optional[str]:
    """Find første fremtidige afgangsdato (ISO YYYY-MM-DD)."""
    if not departures:
        return None
    now = datetime.now(timezone.utc).date()
    future: list[str] = []
    for d in departures:
        if not isinstance(d, dict):
            continue
        start = d.get("start_date") or d.get("startDate")
        if not start:
            continue
        try:
            dt = datetime.fromisoformat(str(start)[:10]).date()
        except ValueError:
            continue
        if dt >= now:
            future.append(dt.isoformat())
    if not future:
        return None
    return min(future)
