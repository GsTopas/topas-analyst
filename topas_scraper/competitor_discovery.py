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
# URL-normalisering + already-mapped filter
# ---------------------------------------------------------------------------

def _normalize_url(url: str) -> str:
    """Lower-case + strip trailing slash + drop fragment/query.
    Bruges til at matche URLs på tværs af capitalization/trailing-slash-varianter."""
    if not url:
        return ""
    u = url.strip().lower().rstrip("/")
    # Drop fragment
    if "#" in u:
        u = u.split("#")[0]
    return u


def _fetch_mapped_urls(conn, operator: str) -> set[str]:
    """Returnér sæt af URLs vi allerede har mappet for denne konkurrent.

    Tjekker BÅDE approved_competitor_targets OG tours-tabellen (for det
    tilfælde hvor en konkurrent er blevet un-approved men data er bevaret).
    Bruges til at filtrere discovery-resultater så vi kun viser NYE ture.

    Operator-matching er fuzzy fordi vi har varianter af konkurrent-navne:
    'Jysk Rejsebureau' vs 'jysk-rejsebureau.dk', 'Gjøa Tours' vs 'gjoa.dk'.
    Vi matcher på domain-fragment.
    """
    # Udled domæne-fragment fra operator-navn (lowercase, drop spaces/symbols)
    op_lower = (operator or "").lower()
    domain_hint = (op_lower
                   .replace(" ", "")
                   .replace("ø", "o").replace("å", "a").replace("æ", "ae")
                   .replace("&", "")
                   .replace(".dk", ""))

    # 1) approved_competitor_targets — primær kilde
    rows = conn.execute("""
        SELECT tour_url
        FROM approved_competitor_targets
        WHERE LOWER(operator) LIKE ?
    """, (f"%{domain_hint}%",)).fetchall()
    urls = {_normalize_url(dict(r)["tour_url"]) for r in rows}

    # 2) tours-tabellen — fanger un-approved historik
    rows = conn.execute("""
        SELECT url
        FROM tours
        WHERE LOWER(operator) LIKE ?
          AND url IS NOT NULL
    """, (f"%{domain_hint}%",)).fetchall()
    urls.update({_normalize_url(dict(r)["url"]) for r in rows})

    # Drop tom streng
    urls.discard("")
    return urls


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
# Lœrings-base — review_decisions med KONTEKST-bevidst klassificering
# ---------------------------------------------------------------------------
#
# Vigtig pointe: Ikke alle reject-reasons er ICP-blokade.
# Brugeren har manuelt klassificeret 185 afvisninger. Nogle er reelt
# kontent-blokade ("Kultur ikke ICP"). Andre er bare screening-mismatch
# ("Forkert geografi" — landet kan stadig være relevant, det var bare
# IKKE en match for den specifikke Topas-tur de sammenlignede mod).
#
# Vi klassificerer hver reason i to spande:
#  - SCREENING_NOISE: ignorér, var bare en mismatch ved screening
#  - CONTENT_BLOCK_*: kun aktiv hvis NY tur har samme træk

def _classify_rejection_reason(reason: str) -> Optional[str]:
    """Returnér kategori for en reject-reason, eller None hvis screening-noise.

    Kategorier:
      - 'kultur'  : kultur/krydstogt/strand/højskole/padel — ikke Topas ICP
      - 'format'  : individuel/solo/self-drive/DMC/ingen fast afgang
      - None      : screening-noise (geografi, længde, manglende data, cykling
                    fremfor vandring, manuel fjernelse — disse er ikke
                    ICP-blokade og skal IGNORERES)
    """
    if not reason:
        return None
    r = reason.lower()

    # === SCREENING-NOISE — alle disse IGNORERES ===
    # Geografi-mismatches: landet kan stadig være relevant
    if "forkert geografi" in r:
        return None
    # Teknisk: ingen pris/dato — vi vil re-screene
    if "manglende data" in r:
        return None
    # Sammenligning-mismatch: turen var bare længere/kortere end Topas-modparten
    if "forkert rejse-længde" in r or "for kort" in r or "for lang" in r:
        return None
    # Unsubscribe fra scraper, ikke kontent-blokade
    if "manuel fjernelse" in r:
        return None
    # CYKLING ER ICP-VALID — afvist fordi sammenlignet med vandre-tur, ikke fordi
    # cykling er irrelevant. Topas har faktisk cykling-ture (Apulien, Atlas, etc).
    if ("cykling" in r or "cykel" in r or "cycling" in r) and "vandr" in r:
        return None
    # "Ingen tur" / "Ingen afgange" — teknisk
    if "ingen tur" in r or "ingen afgange" in r:
        return None
    # "Andet" uden mere info
    if r.startswith("andet ") and "højskole" not in r and "kultur" not in r:
        return None

    # === ÆGTE CONTENT-BLOK ===

    # Kultur-blokade — Topas ICP er vandring/cykling, ikke kultur-ferie
    if ("kultur" in r and not (("kultur ikk" in r) and ("toscan" in r or "umbri" in r))):
        # NB: "Kultur ikke Toscana" er en geografi-detail, ikke kultur-afvisning
        return "kultur"
    if "krydstog" in r:
        return "kultur"
    if "strand" in r:
        return "kultur"
    if "højskole" in r or "højskolen koncept" in r or "sommerhøjskole" in r:
        return "kultur"
    if "padel" in r:
        return "kultur"
    if "kulturferie" in r or "kulturtur" in r:
        return "kultur"

    # Format-blokade — Topas ICP er guided fixed-departure group, ikke individuel
    if "individuel" in r or "selvkør" in r or "self-drive" in r:
        return "format"
    if r.endswith("solo") or " solo" in r or "solo tur" in r:
        return "format"
    if "ungdomsrejs" in r:
        return "format"
    if "dmc baseret" in r or "ingen dansk guide" in r:
        return "format"
    if "ingen fast afgang" in r or "ingen turleder" in r or "ingen fast afrejse" in r:
        return "format"
    if "tog ferie" in r:
        return "format"

    # Default: ukendt → ignorér
    return None


def _build_rejection_patterns(conn) -> list[dict]:
    """Hent afviste kandidater + deres reason + udledt kategori.

    Hver række får 'category'-felt: 'kultur', 'format', eller None (filtreret væk
    af _classify_rejection_reason). Vi smider None-kategorierne væk i bygge-fasen.
    """
    rows = conn.execute("""
        SELECT rd.reason, n8c.tour_name, n8c.tour_category, n8c.search_country
        FROM review_decisions rd
        JOIN n8n_candidates n8c ON n8c.n8n_row_id = rd.target_id
        WHERE rd.action = 'reject'
          AND rd.target_kind = 'n8n_candidate'
          AND rd.reason IS NOT NULL
    """).fetchall()

    out: list[dict] = []
    for r in rows:
        d = dict(r)
        cat = _classify_rejection_reason(d["reason"])
        if cat is None:
            continue  # screening-noise — ikke meningsfuldt at vise som warning
        d["category"] = cat
        out.append(d)
    return out


def _count_rejection_similarity(
    tour: CompetitorTour,
    rejections: list[dict],
) -> tuple[int, list[str]]:
    """Tæl historiske afvisninger der ER relevante for denne tur.

    Vi har allerede filtreret screening-noise ud i _build_rejection_patterns.
    Her tjekker vi:
      - Samme land (location-context)
      - Ny tur klassificeret som hiking/biking allerede (ICP-pass), så
        kategori 'kultur' og 'format' er kun relevante som SVAGE signaler
        (vores classifier kunne være forkert)

    Da hovedfiltret (ICP-pass) allerede har validset turen, er disse
    rejections mere "intel"/"warnings" end hård malus.
    Tæller alle reasons med kategori der matcher samme land.
    """
    if not tour.country:
        return 0, []

    matches: list[str] = []
    for r in rejections:
        rej_country = (r.get("search_country") or "").strip()
        if rej_country != tour.country:
            continue
        # Vi har allerede filtreret screening-noise — alle matches her er ægte
        matches.append(f"[{r['category']}] {r['reason']}")

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


def _departure_validation_score(count: int) -> float:
    """Konverter antal afgange næste 12 mdr til markeds-validerings-score (0-10).

    BEVIDST NON-LINEAR: få afgange signalerer svag markedsefterspørgsel
    (måske test-tur, niche, eller dårligt salg). Flere afgange = valideret
    efterspørgsel = stærkere strategisk signal.

    Tærskel-tier:
      0 afgange        → 0   (ingen markedssignal — tur er måske udgået eller pre-launch)
      1 afgang         → 1   (test-tur / nichetur — næsten ingen validering)
      2-3 afgange      → 3   (svag efterspørgsel — eksperimentel)
      4-6 afgange      → 6   (god efterspørgsel — etableret tur)
      7-11 afgange     → 8   (stærk efterspørgsel — populær tur)
      12+ afgange      → 10  (bestseller — konkurrenten kører den hyppigt)
    """
    if count <= 0:
        return 0.0
    if count == 1:
        return 1.0
    if count <= 3:
        return 3.0
    if count <= 6:
        return 6.0
    if count <= 11:
        return 8.0
    return 10.0


def _score(tour: CompetitorTour, rejected_count: int) -> float:
    """Score-formel for prioritering af gap-ture.

    Formel: score = departure_validation × country_priority − soft rejection_malus

    Maks: 10 × 1.5 = 15 (bestseller i Grønland)
    Min: 0 (clamp)

    Bemærk:
    - departure_validation er NON-LINEAR (se docstring): 1 afgang = svagt signal,
      6+ afgange = stærk markedsvalidering
    - rejection_malus er BLØD (×0.3, max -1.5) — primært intel-signal
    - Screening-noise (geografi, cykling-vs-vandring) er filtreret væk i
      _build_rejection_patterns og giver ingen malus
    """
    base = _departure_validation_score(tour.departure_count_next_12mo)
    country_mult = COUNTRY_PRIORITY.get(tour.country or "", 1.0)
    rejection_malus = min(rejected_count * 0.3, 1.5)
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
    all_found = discovery_result.tours_found

    # Filter ALREADY-MAPPED URLs vaek FOER scraping — discovery handler kun om
    # NYE ture. Hvis URL'en allerede er i approved_competitor_targets, sa har
    # vi allerede en Topas-mapping og tracker den via cron-scrapet.
    from ._pg_conn import connect as pg_connect
    pg_conn = pg_connect()
    already_mapped = _fetch_mapped_urls(pg_conn, operator)
    pre_filter_count = len(all_found)
    all_found = [
        (url, slug) for (url, slug) in all_found
        if _normalize_url(url) not in already_mapped
    ]
    filtered_count = pre_filter_count - len(all_found)
    if filtered_count:
        emit(f"   Filtreret {filtered_count} allerede-mappede URLs vaek "
             f"(de tracker vi allerede via approved_competitor_targets)")

    urls_to_scrape = all_found[:max_urls]
    emit(f"   Fundet {pre_filter_count} URLs via {discovery_result.method_used} · "
         f"{len(all_found)} nye efter filter · scraper top {len(urls_to_scrape)}")

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
    # pg_conn fra step 1 genbruges
    topas_coverage = _build_topas_baseline(pg_conn)
    rejections = _build_rejection_patterns(pg_conn)
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
        "urls_discovered": pre_filter_count,
        "urls_after_mapped_filter": len(all_found),
        "tours_classified": len(tours),
        "icp_passing": len(icp_tours),
        "gaps": gaps,
        "stats": {
            "method": discovery_result.method_used,
            "errors": errors[:20],
            "max_urls_capped": len(all_found) > max_urls,
            "already_mapped_filtered": filtered_count,
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
