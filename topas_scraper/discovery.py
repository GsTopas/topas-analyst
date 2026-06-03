"""
Discovery — find every tour-URL on a competitor's site.

Strategy:
1. Try sitemap.xml first. Fast, free, no Firecrawl credits used.
2. If no sitemap or it's incomplete, crawl listing pages with Firecrawl's
   `/v2/map` endpoint (cheaper than full scrape).
3. Filter the resulting URL set down to URLs that look like tour-detail pages.

The output is a list of (operator, tour_url, tour_slug, discovery_method)
tuples that get upserted into catalog_db.

This module deliberately does NOT classify or extract details. It just
finds candidate URLs. Classification comes later (classifier.py) using
a much smaller fraction of the URL set after AI-screening.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse, urljoin

import requests

from .client import FirecrawlClient


# ---------------------------------------------------------------------------
# URL filtering — which URLs look like tour-detail pages?
# ---------------------------------------------------------------------------
# Each operator structures their site differently. We keep filters per-operator
# to avoid mistaking blog posts / category pages for actual tour-detail pages.

TOUR_URL_PATTERNS: dict[str, list[re.Pattern]] = {
    "Topas": [
        # Topas tour-pages have format /landenavn-turbeskrivelse/
        re.compile(r"^https?://www\.topas\.dk/[a-z0-9æøå]+-[a-z0-9æøå-]+/?$", re.IGNORECASE),
    ],
    "Smilrejser": [
        # Pattern: /landenavn/tour-slug
        re.compile(r"^https?://(?:www\.)?smilrejser\.dk/[a-z0-9æøå-]+/[a-z0-9æøå-]+/?$", re.IGNORECASE),
    ],
    "Jysk Rejsebureau": [
        # Pattern: /{land}/med-dansk-rejseleder/{slug}/
        # Jysk's sitemap indeholder ~2700 URLs (mest payment/kvitterings-sider).
        # ALLE rigtige guided tours ligger under /{land}/med-dansk-rejseleder/
        # som passer praecis paa Topas ICP (dansk rejseleder, fixed-departure).
        re.compile(
            r"^https?://(?:www\.)?jysk-rejsebureau\.dk/[a-z0-9æøå-]+/med-dansk-rejseleder/[a-z0-9æøå.-]+/?$",
            re.IGNORECASE,
        ),
    ],
    "Viktors Farmor": [
        # Pattern: /rejsemal/region/land/tour-slug
        re.compile(r"^https?://(?:www\.)?viktorsfarmor\.dk/rejsemal/[a-z0-9æøå-]+/[a-z0-9æøå-]+/[a-z0-9æøå-]+/?$", re.IGNORECASE),
    ],
    "Stjernegaard Rejser": [
        # Stjernegaard bruger /{land}/{kategori}/{slug}/priser-og-datoer/
        # Vi vil bevidst KUN matche /priser-og-datoer/-URLs fordi det er der
        # afgangsdatoer + priser ligger (forside-slug har thin data).
        re.compile(
            r"^https?://(?:www\.)?stjernegaard-rejser\.dk/[a-z0-9æøå-]+/[a-z0-9æøå-]+/[a-z0-9æøå-]+/priser-og-datoer/?$",
            re.IGNORECASE,
        ),
    ],
    "Albatros Travel": [
        # Albatros bruger albatros.dk/rejser/{slug} med optional ?variant= for
        # specifik afgang. Sitemap.xml returnerer 0 bytes (tom), saa vi er
        # afhaengige af Firecrawl /map. Regex tillader query-string fordi
        # Firecrawl /map kan returnere variant-URLs; deduplication sker via
        # _slug_from_url (som stripper query).
        re.compile(
            r"^https?://(?:www\.)?albatros\.dk/rejser/[a-z0-9æøå.-]+/?(?:\?.*)?$",
            re.IGNORECASE,
        ),
    ],
    "Nilles & Gislev": [
        re.compile(r"^https?://(?:www\.)?nillesgislev\.dk/[a-z0-9æøå-]+/[a-z0-9æøå-]+/?$", re.IGNORECASE),
    ],
    "Ruby Rejser": [
        re.compile(r"^https?://(?:www\.)?ruby-rejser\.dk/[a-z0-9æøå-]+/[a-z0-9æøå-]+\.html$", re.IGNORECASE),
    ],
    "Vagabond Tours": [
        # Vagabond migrerede fra ASP.NET til WordPress (~2025). Nye URLs er
        # vagabondtours.dk/tours/{slug}/ — fx /tours/gendarmstien-padborg-broager/
        # eller /tours/luksus-vandreferie-i-tyskland-malerweg-6-dage/
        # NB: sitemap har ogsaa /tours/test* og /tours/ (index) som ICP-filter
        # vil weede ud baseret paa thin data.
        re.compile(
            r"^https?://(?:www\.)?vagabondtours\.dk/tours/[a-z0-9æøå.-]+/?$",
            re.IGNORECASE,
        ),
    ],
    "Kipling Travel": [
        # Kipling bruger kiplingtravel.dk/rejser/{kontinent}/{land}/{slug}
        # Eksempel: kiplingtravel.dk/rejser/afrika/marokko/toubkal-sommerbestigning
        re.compile(
            r"^https?://(?:www\.)?kiplingtravel\.dk/rejser/[a-z0-9æøå-]+/[a-z0-9æøå-]+/[a-z0-9æøå.-]+/?$",
            re.IGNORECASE,
        ),
    ],
    "Gjøa Tours": [
        # Gjoea bruger /temarejser/{land}/{slug}/
        # Eksempel: gjoa.dk/temarejser/italien/det-sicilianske-foraar-vandring-i-monti-sicani-...
        re.compile(
            r"^https?://(?:www\.)?gjoa\.dk/temarejser/[a-z0-9æøå-]+/[a-z0-9æøå.-]+/?$",
            re.IGNORECASE,
        ),
    ],
    "Fyrholt Rejser": [
        # Fyrholt bruger /rejser/{slug}/
        # Eksempel: fyrholtrejser.dk/rejser/la-gomera/
        re.compile(
            r"^https?://(?:www\.)?fyrholtrejser\.dk/rejser/[a-z0-9æøå.-]+/?$",
            re.IGNORECASE,
        ),
    ],
    "Bering Travel": [
        # Bering bruger beringtravel.com (.com TLD!) med locale-prefiks /da/.
        # URL-shape: /da/{kategori}/{land}/{slug}
        # Eksempler: /da/vandreferie/albanien/vandreferie-albaniens-alper
        #            /da/cykelferie/danmark/cykelferie-koebenhavn-bornholm
        # 321 ture totalt i /da-locale (sept 2026). Kategori-roots og
        # land-pages (depth 1-2) er ikke ture — kun depth-3 URLs matcher.
        re.compile(
            r"^https?://(?:www\.)?beringtravel\.com/da/[a-z0-9æøå-]+/[a-z0-9æøå-]+/[a-z0-9æøå.-]+/?$",
            re.IGNORECASE,
        ),
    ],
}


# URL fragments to exclude even if they match a tour pattern. Catches
# common false positives like blog posts, category index pages, login,
# og kvitterings-/betalings-/booking-sider (Jysk's sitemap har ~2000 af dem).
EXCLUDE_FRAGMENTS = {
    "/blog/", "/nyheder/", "/news/",
    "/login", "/konto", "/min-side", "/kontakt",
    "/cookies", "/privatlivspolitik", "/handelsbetingelser",
    "/om-os", "/karriere", "/job",
    "/search", "/sog",
    "/category/", "/tag/",
    "/wp-content/", "/wp-admin/", "/wp-json/",
    ".pdf", ".jpg", ".png", ".css", ".js",
    # Payment / kvittering / booking-flow — typisk transient pages
    "/betaling", "/payment", "/pay/", "/checkout",
    "/kvittering", "/receipt", "/tak-for", "/thank-you", "/thankyou",
    "/bestilling", "/booket", "/booking-bekraeftelse",
    "/vilkaar", "/vilkar", "/terms",
    "/fejl", "/error", "/404", "/500",
    "/afregning", "/faktura",
    "/info-moede", "/info-mode", "/booket-mode",
}


def is_likely_tour_url(operator: str, url: str) -> bool:
    """Test if a URL matches the tour-detail pattern for the given operator.

    For known operators (in TOUR_URL_PATTERNS), apply the specific regex.
    For unknown operators that look like a domain (typisk custom-domain via UI
    hvor operator = 'intrepidtravel.com'), brug permissiv heuristik:
      - URL skal vaere paa samme domaene som operator
      - URL maa ikke matche EXCLUDE_FRAGMENTS (payment-sider, blog etc)
      - URL skal have 1+ path-segment (ikke bare homepage)
    Saa kan ICP-classifier lave finkornet filtrering bagefter."""
    url_lower = url.lower()
    if any(frag in url_lower for frag in EXCLUDE_FRAGMENTS):
        return False

    patterns = TOUR_URL_PATTERNS.get(operator)
    if patterns:
        return any(p.match(url) for p in patterns)

    # Unknown operator — kun permissiv hvis operator ligner et domaene
    op = (operator or "").lower().strip()
    is_domain_like = "." in op and " " not in op
    if not is_domain_like:
        return False

    # URL skal vaere paa samme domaene (med eller uden www.)
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    if not (host == op or host == f"www.{op}" or host.endswith(f".{op}")):
        return False

    # Skal have 1+ path-segment (ikke bare homepage)
    path = parsed.path.strip("/")
    if not path:
        return False

    return True


# ---------------------------------------------------------------------------
# Strategy 1 — sitemap.xml
# ---------------------------------------------------------------------------

def fetch_sitemap_urls(sitemap_url: str, timeout: int = 15) -> Optional[list[str]]:
    """
    Fetch a sitemap.xml (or sitemap index) and return all URLs.
    Returns None if fetch fails. Returns [] if sitemap is empty.
    Recursively expands sitemap-indexes (sitemaps that point to other sitemaps).
    Handles both plain XML and gzip-compressed sitemaps (.gz or magic-byte detected).
    """
    try:
        resp = requests.get(
            sitemap_url,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; topas-scraper/1.0)",
                "Accept-Encoding": "gzip, deflate",
            },
            allow_redirects=True,
        )
        resp.raise_for_status()
    except (requests.RequestException, requests.Timeout):
        return None

    # Decompress gzip if URL ends with .gz OR content starts with gzip magic bytes.
    # requests library auto-decompresses Content-Encoding: gzip transparently, men
    # explicitte .gz-filer arriverer ofte med Content-Type: application/gzip og
    # rå gzip-bytes (uden Content-Encoding header).
    content = resp.content
    is_gzip = (
        sitemap_url.lower().endswith(".gz")
        or (len(content) >= 2 and content[:2] == b"\x1f\x8b")
    )
    if is_gzip:
        import gzip
        try:
            content = gzip.decompress(content)
        except (OSError, EOFError):
            return None

    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return None

    # Strip XML namespace for simpler tag matching
    ns = root.tag.split("}")[0] + "}" if "}" in root.tag else ""
    urls: list[str] = []

    if root.tag == f"{ns}sitemapindex":
        # Index of sitemaps — recurse
        for sm in root.findall(f"{ns}sitemap/{ns}loc"):
            child_url = (sm.text or "").strip()
            if child_url:
                child_urls = fetch_sitemap_urls(child_url, timeout=timeout)
                if child_urls:
                    urls.extend(child_urls)
    elif root.tag == f"{ns}urlset":
        for url_el in root.findall(f"{ns}url/{ns}loc"):
            url = (url_el.text or "").strip()
            if url:
                urls.append(url)

    return urls


def discover_via_sitemap(
    operator: str,
    sitemap_url: str,
) -> Optional[list[tuple[str, str]]]:
    """
    Discover tour-URLs via sitemap.

    Tries sitemap_url first. If that fails, tries robots.txt for Sitemap-hint
    (mange sites lister deres sitemap-sti der, fx Jysk har sitemap.xml.gz).

    Returns list of (url, slug) tuples, or None if sitemap couldn't be loaded.
    Filters URLs to tour-detail pattern for the operator.
    """
    all_urls = fetch_sitemap_urls(sitemap_url)
    if all_urls is None or not all_urls:
        # Fallback: tjek robots.txt for Sitemap:-direktiv
        all_urls = _try_robots_sitemap(sitemap_url)
        if all_urls is None:
            return None

    tour_urls = [u for u in all_urls if is_likely_tour_url(operator, u)]

    # Defensiv fallback: sitemap returnerede URLs, men ingen matcher operator's
    # tour-URL-pattern. Ofte er sitemap'en forkert (Ruby's /sitemap.xml havde
    # URLs fra skiinstruktor.no-soesterprojekt). Proev robots.txt for alternativ
    # sitemap inden vi giver op.
    if not tour_urls:
        robots_urls = _try_robots_sitemap(sitemap_url)
        if robots_urls:
            combined = list({*all_urls, *robots_urls})
            tour_urls = [u for u in combined if is_likely_tour_url(operator, u)]

    return _dedupe_by_slug([(u, _slug_from_url(u)) for u in tour_urls])


def _try_robots_sitemap(original_sitemap_url: str) -> Optional[list[str]]:
    """Forsøg at finde alternativ sitemap-sti via robots.txt.

    Bruges som fallback når den primære sitemap-URL fejler (fx Jysk har
    sitemap.xml.gz i stedet for sitemap.xml).
    """
    parsed = urlparse(original_sitemap_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    try:
        resp = requests.get(
            robots_url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; topas-scraper/1.0)"},
        )
        resp.raise_for_status()
    except (requests.RequestException, requests.Timeout):
        return None

    # Find alle "Sitemap: <url>"-linjer
    sitemap_lines = re.findall(r"^Sitemap:\s*(\S+)", resp.text, re.MULTILINE | re.IGNORECASE)
    if not sitemap_lines:
        return None

    # Saml URLs fra alle annoncerede sitemaps
    all_urls: list[str] = []
    for sm_url in sitemap_lines:
        urls = fetch_sitemap_urls(sm_url.strip())
        if urls:
            all_urls.extend(urls)

    return all_urls if all_urls else None


# ---------------------------------------------------------------------------
# Strategy 2 — Firecrawl /map
# ---------------------------------------------------------------------------

def discover_via_firecrawl_map(
    operator: str,
    homepage_url: str,
    client: Optional[FirecrawlClient] = None,
) -> list[tuple[str, str]]:
    """
    Discover tour-URLs via Firecrawl's /map endpoint.

    Firecrawl /map returns all URLs Firecrawl can find on the site without
    fully scraping each one. Cheaper than crawl. We filter the result to
    tour-detail URLs for this operator.
    """
    if client is None:
        client = FirecrawlClient()

    try:
        # Firecrawl SDK exposes .map() for this. Wrap in try since the SDK
        # surface has changed across versions.
        result = client.client.map(homepage_url)
        # Different SDK versions return different shapes — handle both.
        if hasattr(result, "links"):
            all_urls = result.links
        elif isinstance(result, dict) and "links" in result:
            all_urls = result["links"]
        elif isinstance(result, list):
            all_urls = result
        else:
            all_urls = []
    except Exception as exc:
        # Don't crash discovery — return empty and let caller log it.
        print(f"  Firecrawl map failed for {operator}: {exc}")
        return []

    tour_urls = [u for u in all_urls if is_likely_tour_url(operator, u)]
    return _dedupe_by_slug([(u, _slug_from_url(u)) for u in tour_urls])


def _dedupe_by_slug(items: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Behold kun foerste URL pr. slug. Albatros's /map kan returnere flere
    variant-URLs (?variant=XXX) for samme tur — vi vil scrape canonical-URL
    en gang i stedet for 50 gange."""
    seen_slugs: set[str] = set()
    out: list[tuple[str, str]] = []
    for url, slug in items:
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        # Foretraek URL uden query-string (canonical) hvis bade canonical og
        # variant er i listen — naar vi alligevel kun beholder en, taeller den
        # tidligere ankommer. For sikkerheds skyld: strip query her saa vi
        # scraper canonical-siden uden variant-noise.
        canonical = url.split("?", 1)[0]
        out.append((canonical, slug))
    return out


# ---------------------------------------------------------------------------
# Top-level discover()
# ---------------------------------------------------------------------------

@dataclass
class DiscoveryResult:
    operator: str
    tours_found: list[tuple[str, str]]    # (url, slug)
    method_used: str                       # 'sitemap' | 'firecrawl-map' | 'failed'
    notes: str = ""
    raw_url_count: int = 0                 # antal URLs FOER is_likely_tour_url-filter
    fetch_error: str = ""                  # populeret hvis sitemap/map fejlede


def discover_operator_tours(
    operator: str,
    homepage_url: str,
    sitemap_url: Optional[str] = None,
    firecrawl_client: Optional[FirecrawlClient] = None,
) -> DiscoveryResult:
    """
    Top-level discovery for one operator.

    Tries sitemap first (free + fast), falls back to Firecrawl /map if sitemap
    is missing or returns suspiciously few results.
    """
    sitemap_raw_count = 0
    sitemap_error = ""
    sitemap_results: Optional[list[tuple[str, str]]] = None

    # Strategy 1 — sitemap
    if sitemap_url:
        try:
            sitemap_results, sitemap_raw_count = _discover_via_sitemap_with_count(
                operator, sitemap_url
            )
        except Exception as exc:
            sitemap_error = f"sitemap fetch error: {type(exc).__name__}: {exc}"
            sitemap_results = None

        if sitemap_results is not None and len(sitemap_results) >= 5:
            return DiscoveryResult(
                operator=operator,
                tours_found=sitemap_results,
                method_used="sitemap",
                notes=(f"{len(sitemap_results)} tour-URLs from sitemap "
                       f"(filtered from {sitemap_raw_count} raw URLs)"),
                raw_url_count=sitemap_raw_count,
            )

    # Strategy 2 — Firecrawl /map
    map_raw_count = 0
    map_error = ""
    map_results: list[tuple[str, str]] = []
    try:
        map_results, map_raw_count = _discover_via_firecrawl_map_with_count(
            operator, homepage_url, firecrawl_client
        )
    except Exception as exc:
        map_error = f"firecrawl /map error: {type(exc).__name__}: {exc}"

    if map_results:
        return DiscoveryResult(
            operator=operator,
            tours_found=map_results,
            method_used="firecrawl-map",
            notes=(f"{len(map_results)} tour-URLs from Firecrawl /map "
                   f"(filtered from {map_raw_count} raw URLs)"),
            raw_url_count=map_raw_count,
        )

    # Begge fejlede — saml diagnostik
    combined_raw = sitemap_raw_count + map_raw_count
    diag_parts = []
    if sitemap_url:
        diag_parts.append(
            f"sitemap returned {sitemap_raw_count} URLs"
            + (f" — {sitemap_error}" if sitemap_error else "")
        )
    diag_parts.append(
        f"Firecrawl /map returned {map_raw_count} URLs"
        + (f" — {map_error}" if map_error else "")
    )
    if combined_raw > 0:
        diag_parts.append(
            f"but 0 matched is_likely_tour_url for operator='{operator}' "
            "(URL-pattern mismatch?)"
        )
    notes = " · ".join(diag_parts)

    return DiscoveryResult(
        operator=operator,
        tours_found=[],
        method_used="failed",
        notes=notes,
        raw_url_count=combined_raw,
        fetch_error=(sitemap_error or map_error or ""),
    )


def _discover_via_sitemap_with_count(
    operator: str, sitemap_url: str,
) -> tuple[Optional[list[tuple[str, str]]], int]:
    """Wrap discover_via_sitemap saa vi ogsaa returnerer raw URL-count (foer filter)."""
    all_urls = fetch_sitemap_urls(sitemap_url)
    raw_count = len(all_urls) if all_urls else 0

    if all_urls is None or not all_urls:
        all_urls = _try_robots_sitemap(sitemap_url)
        if all_urls is None:
            return None, 0
        raw_count = len(all_urls)

    tour_urls = [u for u in all_urls if is_likely_tour_url(operator, u)]

    # Defensiv fallback (samme som discover_via_sitemap)
    if not tour_urls:
        robots_urls = _try_robots_sitemap(sitemap_url)
        if robots_urls:
            combined = list({*all_urls, *robots_urls})
            raw_count = len(combined)
            tour_urls = [u for u in combined if is_likely_tour_url(operator, u)]

    return _dedupe_by_slug([(u, _slug_from_url(u)) for u in tour_urls]), raw_count


def _discover_via_firecrawl_map_with_count(
    operator: str, homepage_url: str, client: Optional[FirecrawlClient] = None,
) -> tuple[list[tuple[str, str]], int]:
    """Wrap discover_via_firecrawl_map saa vi ogsaa returnerer raw URL-count."""
    if client is None:
        client = FirecrawlClient()

    result = client.client.map(homepage_url)  # exceptions propagate to caller
    if hasattr(result, "links"):
        all_urls = result.links
    elif isinstance(result, dict) and "links" in result:
        all_urls = result["links"]
    elif isinstance(result, list):
        all_urls = result
    else:
        all_urls = []

    raw_count = len(all_urls)
    tour_urls = [u for u in all_urls if is_likely_tour_url(operator, u)]
    return _dedupe_by_slug([(u, _slug_from_url(u)) for u in tour_urls]), raw_count


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slug_from_url(url: str) -> str:
    """Extract a slug-ish identifier from a URL path."""
    path = urlparse(url).path.strip("/")
    # Strip trailing slashes and file extensions
    path = re.sub(r"\.html?$", "", path)
    if not path:
        return "(homepage)"
    return path.split("/")[-1]
