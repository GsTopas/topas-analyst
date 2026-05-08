"""
Topas catalog: seed-URL based discovery.

ARCHITECTURAL DECISION:
We tried four approaches to auto-discover Topas's Fællesrejser-med-turleder:
  1. Sitemap.xml — incomplete (missing many tours, lots of junk)
  2. Search filter URL with no scroll — got 7 of ~37 tours
  3. Search filter URL with 20× regular scroll — got 13 tours
  4. Search filter URL with 15× JS-injection scroll-to-bottom — got 17 tours

Pattern: diminishing returns. We hit a structural limit in Firecrawl's single-
call architecture (likely browser memory cap or LLM context limit on the final
DOM). More iteration won't break through.

Decision: stop fighting auto-discovery. Topas's portfolio changes slowly
(maybe 2-5 tours/year). Manual seed list + manual "add new tour" input is
operationally trivial. Per the project's "data acquisition principle" — tool
matches analyst's reach — we trust the analyst (user) to maintain the URL
list as Topas's portfolio evolves.

This module:
  - TOPAS_SEED_URLS: the canonical list, manually curated from /faellesrejser/
  - scrape_tour_metadata(): light-scrape a single URL for catalog metadata
  - fetch_topas_catalog(): bulk-scrape the seed list (used by "Refresh" button)

Cost: ~$0.005 per URL × 49 URLs = ~$0.25 for full refresh. Each "add new tour"
costs ~$0.005. Negligible.
"""

from __future__ import annotations

from typing import Callable, Optional


# ---------------------------------------------------------------------------
# Seed URL list — manually curated from topas.dk/faellesrejser/
# ---------------------------------------------------------------------------
# Updated: 2026-05-05 (49 URLs visible at /faellesrejser/)
# To add a new tour: append URL here, OR use the "Tilføj ny tur" input in
# the Streamlit catalog page (recommended — preserves audit trail).

TOPAS_SEED_URLS: list[str] = [
    # Asia
    "https://www.topas.dk/nordvietnam-eventyrlig-rundrejse/",
    "https://www.topas.dk/nepal-himalaya-trekking-til-everest-base-camp-og-cho-la-pas/",
    "https://www.topas.dk/nepal-trekking-i-langtang-og-gosaikunda-lake/",
    "https://www.topas.dk/nepal-himalayas-smukkeste-trekking-rundt-om-mount-manaslu/",
    "https://www.topas.dk/nepal-vandreferie-i-himalayas-spektakulaere-annapurna-region/",
    "https://www.topas.dk/sri-lanka-en-kulturhistorisk-rundrejse-med-mystik-og-frodig-natur/",

    # Europe — Italy
    "https://www.topas.dk/italien-amalfikystens-unikke-skoenhed/",
    "https://www.topas.dk/italien-kultur-og-vandring-i-italiens-groenne-hjerte-umbrien/",
    "https://www.topas.dk/italien-cykelferie-i-naturskoenne-og-kulturstaerke-apulien/",
    "https://www.topas.dk/italien-la-fontanella-en-aegte-perle-i-toscana/",
    "https://www.topas.dk/italien-vulkanvandring-og-panoramaudsigter-paa-sicilien/",
    "https://www.topas.dk/italien-saeteridyl-og-bjergtinder-i-dolomitterne/",
    "https://www.topas.dk/italien-fra-hytte-til-hytte-i-de-uspolerede-dolomitter/",
    "https://www.topas.dk/italien-vandreferie-i-de-toscanske-appenniner/",

    # Europe — France
    "https://www.topas.dk/frankrig/korsika/skoenhedens-oe/",
    "https://www.topas.dk/korsika-yoga-og-vandring/",
    "https://www.topas.dk/frankrig-vandring-i-pyrenaeernes-storslaaede-natur/",

    # Europe — Spain
    "https://www.topas.dk/spanien-la-gomera-et-vandremekka-for-naturelskere/",
    "https://www.topas.dk/spanien-vandreferie-i-andalusien/",
    "https://www.topas.dk/spanien-caminovandringer-og-unesco-sites-i-malaga/",

    # Europe — Croatia
    "https://www.topas.dk/kroatien-sejlads-og-cykling-i-dalmatiens-oehav/",
    "https://www.topas.dk/kroatien-sejlads-og-vandring-i-dalmatiens-oehav/",
    "https://www.topas.dk/kroatien-sejlads-og-vandring-i-det-kroatiske-oehav/",

    # Europe — Other
    "https://www.topas.dk/norge-vinterferie-paa-langrend-i-gudbrandsdalen/",
    "https://www.topas.dk/cypern-aktiv-ferie-med-sol-strand-og-vandring/",
    "https://www.topas.dk/vandreferie-i-irlands-vilde-nordvest/",
    "https://www.topas.dk/albanien-raa-og-autentisk-trekking-i-de-albanske-alper/",
    "https://www.topas.dk/madeira-majestaetiske-tinder-og-levadavandring/",
    "https://www.topas.dk/portugal-kultur-og-vandring-i-peneda-geres-nationalpark/",
    "https://www.topas.dk/skotland-trekking-i-det-skotske-hoejland/",
    "https://www.topas.dk/wales-vandring-i-magiske-snowdonia/",

    # Africa
    "https://www.topas.dk/marokko-eksotisk-cykelferie-blandt-palmelunde-og-sandmiler/",
    "https://www.topas.dk/marokko-trekking-i-hoeje-atlas-bjergene-til-toppen-af-mount-toubkal/",

    # Americas
    "https://www.topas.dk/peru-titicaca-cusco-og-trekking-ved-machu-picchu/",
    "https://www.topas.dk/costa-rica-en-uforglemmelig-rundrejse-i-latinamerikas-naturparadis/",
    "https://www.topas.dk/patagonien-aktiv-rundrejse-til-argentina-og-chiles-enestaaende-nationalparker/",

    # Oceania
    "https://www.topas.dk/new-zealand-aktiv-rundrejse-paa-sydoeen/",

    # Greenland (large portfolio)
    "https://www.topas.dk/groenland-den-ultimative-rundrejse-i-diskobugten/",
    "https://www.topas.dk/arktisk-velvaere-nuuk-og-rundrejse-i-diskobugten-fra-billund/",
    "https://www.topas.dk/groenland-arktisk-velvaere-rundrejse-i-diskobugten/",
    "https://www.topas.dk/groenland-forsommer-og-midnatssol-i-diskobugten/",
    "https://www.topas.dk/groenland-eventyrlig-rundrejse-i-diskobugten/",
    "https://www.topas.dk/groenland-hvalsafari-og-hundeslaede-paa-diskooeen/",
    "https://www.topas.dk/groenland-isbjerge-nordlys-og-kulturarv-i-ilulissat/",
    "https://www.topas.dk/groenland-nytaar-og-nordlys-i-ilulissat/",
    "https://www.topas.dk/groenland-sensommer-nord-for-polarcirklen/",
    "https://www.topas.dk/groenland-rundrejse-fra-bygd-til-vildmark/",
    "https://www.topas.dk/groenland-vintermagi-i-ilulissat-isbjerge-kultur-og-komfort/",
    "https://www.topas.dk/groenland/trekking-fra-indlandsisen-til-diskobugten/",
]


# ---------------------------------------------------------------------------
# Schema for per-product metadata extraction
# ---------------------------------------------------------------------------
# These URLs all come from /faellesrejser/, so we know they're Fællesrejser-
# med-turleder by definition. We don't need to verify tour_format or
# has_danish_guide — just extract the catalog metadata.

TOPAS_PRODUCT_METADATA_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "tour_code": {
            "type": ["string", "null"],
            "description": (
                "Topas's internal tour code (turkode), 3-5 uppercase letters "
                "like 'NPAP', 'PTMD', 'VNSN', 'ITLA'. Look for 'Turkode:' label, "
                "or in URL/breadcrumbs/page metadata. If genuinely not present, null."
            ),
        },
        "tour_name": {
            "type": "string",
            "description": "The tour title as shown on the page (the H1 headline).",
        },
        "country": {
            "type": ["string", "null"],
            "description": (
                "Primary destination country (e.g. 'Italien', 'Nepal', 'Grønland'). "
                "If multi-country, the first/primary one."
            ),
        },
        "duration_days": {
            "type": ["integer", "null"],
            "description": "Duration in days. '14 dage' → 14.",
        },
        "from_price_dkk": {
            "type": ["integer", "null"],
            "description": "Lowest 'fra X kr.' price visible, integer DKK.",
        },
        "audience_segment": {
            "type": "string",
            "enum": ["Åben", "30-50 år", "Familie", "Andet"],
            "description": (
                "Audience segment if explicitly tagged. '30-50 år' is a "
                "Topas-specific segment. Default 'Åben' if no tag visible."
            ),
        },
    },
    "required": ["tour_name"],
}


# ---------------------------------------------------------------------------
# Single-URL scrape
# ---------------------------------------------------------------------------

def scrape_tour_metadata(
    client,
    url: str,
    on_progress: Optional[Callable[[str], None]] = None,
) -> Optional[dict]:
    """Light-scrape a single Topas product URL for catalog metadata.

    Returns normalized tour dict, or None on failure. Cost: ~$0.005.
    """
    def emit(msg: str) -> None:
        if on_progress is not None:
            on_progress(msg)

    if not url or not url.startswith("https://www.topas.dk/"):
        emit(f"Ugyldigt URL: {url}")
        return None

    overrides = {
        "wait_for": 2000,           # short wait — server-rendered pages
        "only_main_content": False,
    }

    try:
        result = client.scrape(
            url,
            overrides=overrides,
            schema=TOPAS_PRODUCT_METADATA_SCHEMA,
        )
    except Exception as e:
        emit(f"Scrape fejlede: {e}")
        return None

    if not result.success:
        emit(f"Scrape unsuccessful: {result.error}")
        return None

    if not result.extracted:
        emit(f"Ingen metadata kunne ekstraheres fra {_url_slug(url)}")
        return None

    raw = result.extracted

    return {
        "tour_code": _safe_str(raw.get("tour_code")),
        "tour_name": _safe_str(raw.get("tour_name")) or "(ingen navn)",
        "url": url,
        "country": _safe_str(raw.get("country")),
        "duration_days": _safe_int(raw.get("duration_days")),
        "from_price_dkk": _safe_int(raw.get("from_price_dkk")),
        "audience_segment": _safe_str(raw.get("audience_segment")) or "Åben",
    }


# ---------------------------------------------------------------------------
# Bulk-scrape from seed list
# ---------------------------------------------------------------------------

def fetch_topas_catalog(
    client,
    urls: Optional[list[str]] = None,
    on_progress: Optional[Callable[[str], None]] = None,
) -> list[dict]:
    """Scrape the Topas catalog from a list of URLs.

    Args:
        client: FirecrawlClient instance.
        urls: Optional URL list. If None, uses TOPAS_SEED_URLS.
        on_progress: optional callback for status messages.

    Returns:
        List of normalized tour dicts, deduplicated by tour_code.
    """
    def emit(msg: str) -> None:
        if on_progress is not None:
            on_progress(msg)

    target_urls = urls if urls is not None else TOPAS_SEED_URLS
    emit(f"Henter metadata for {len(target_urls)} ture...")

    tours: list[dict] = []
    failed: list[str] = []

    for i, url in enumerate(target_urls, 1):
        emit(f"[{i}/{len(target_urls)}] {_url_slug(url)}...")
        tour = scrape_tour_metadata(client, url)
        if tour:
            tours.append(tour)
        else:
            failed.append(url)

    emit(f"Hentede metadata for {len(tours)}/{len(target_urls)} ture")
    if failed:
        emit(f"Fejlede ({len(failed)}): {', '.join(_url_slug(u) for u in failed[:5])}"
             + ("..." if len(failed) > 5 else ""))

    # Dedup on tour_code
    deduped, dup_count = _dedup_by_code(tours)
    if dup_count > 0:
        emit(f"Færdig: {len(deduped)} unikke ture ({dup_count} duplikater fjernet)")
    else:
        emit(f"Færdig: {len(deduped)} unikke ture")

    return deduped


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

def _dedup_by_code(tours: list[dict]) -> tuple[list[dict], int]:
    """Group tours by tour_code, keep most complete per code.

    Tours without tour_code are kept as-is (each unique by URL).
    """
    by_code: dict[str, dict] = {}
    no_code: list[dict] = []
    duplicates_removed = 0

    for tour in tours:
        code = tour.get("tour_code")
        if not code:
            no_code.append(tour)
            continue

        if code in by_code:
            existing = by_code[code]
            if _completeness_score(tour) > _completeness_score(existing):
                by_code[code] = tour
            duplicates_removed += 1
        else:
            by_code[code] = tour

    return list(by_code.values()) + no_code, duplicates_removed


def _completeness_score(tour: dict) -> int:
    """Score by data completeness — higher is better."""
    score = 0
    if tour.get("country"):
        score += 1
    if tour.get("duration_days"):
        score += 1
    if tour.get("from_price_dkk"):
        score += 1
    if tour.get("audience_segment"):
        score += 1
    name = tour.get("tour_name", "") or ""
    if len(name) > 20:
        score += 1
    if tour.get("audience_segment") == "Åben":
        score += 2  # Prefer Åben over 30-50 år when codes collide
    return score


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _url_slug(url: str) -> str:
    return url.rstrip("/").rsplit("/", 1)[-1] or url


def _safe_str(value) -> Optional[str]:
    if not isinstance(value, str):
        return None
    s = value.strip()
    return s if s else None


def _safe_int(value) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float):
        return int(value) if value > 0 else None
    if isinstance(value, str):
        cleaned = "".join(c for c in value if c.isdigit())
        if cleaned:
            try:
                n = int(cleaned)
                return n if n > 0 else None
            except ValueError:
                return None
    return None
