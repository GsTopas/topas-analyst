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
        # Pattern: /landenavn/category/tour-slug or /landenavn/tour-slug
        re.compile(r"^https?://(?:www\.)?jysk-rejsebureau\.dk/[a-z0-9æøå-]+/(?:[a-z0-9æøå-]+/)*[a-z0-9æøå-]+/?$", re.IGNORECASE),
    ],
    "Viktors Farmor": [
        # Pattern: /rejsemal/region/land/tour-slug
        re.compile(r"^https?://(?:www\.)?viktorsfarmor\.dk/rejsemal/[a-z0-9æøå-]+/[a-z0-9æøå-]+/[a-z0-9æøå-]+/?$", re.IGNORECASE),
    ],
    "Stjernegaard Rejser": [
        re.compile(r"^https?://(?:www\.)?stjernegaard-rejser\.dk/[a-z0-9æøå-]+/[a-z0-9æøå-]+/?$", re.IGNORECASE),
    ],
    "Albatros Travel": [
        # Albatros uses /rejsemaal/destination/tour-slug or /tema/tour-slug
        re.compile(r"^https?://(?:www\.)?albatros-travel\.dk/(?:rejsemaal|tema)/[a-z0-9æøå-]+/[a-z0-9æøå-]+/?$", re.IGNORECASE),
        re.compile(r"^https?://(?:www\.)?albatros-travel\.dk/[a-z0-9æøå-]+/[a-z0-9æøå-]+-rejse/?$", re.IGNORECASE),
    ],
    "Nilles & Gislev": [
        re.compile(r"^https?://(?:www\.)?nillesgislev\.dk/[a-z0-9æøå-]+/[a-z0-9æøå-]+/?$", re.IGNORECASE),
    ],
    "Ruby Rejser": [
        re.compile(r"^https?://(?:www\.)?ruby-rejser\.dk/[a-z0-9æøå-]+/[a-z0-9æøå-]+\.html$", re.IGNORECASE),
    ],
    "Vagabond Tours": [
        re.compile(r"^https?://(?:www\.)?vagabondtours\.dk/rejser/[a-z0-9æøå-]+/[a-z0-9æøå-]+/?$", re.IGNORECASE),
    ],
    "Kipling Travel": [
        re.compile(r"^https?://(?:www\.)?kipling\.dk/rejser/[a-z0-9æøå-]+/[a-z0-9æøå-]+/?$", re.IGNORECASE),
    ],
}


# URL fragments to exclude even if they match a tour pattern. Catches
# common false positives like blog posts, category index pages, login.
EXCLUDE_FRAGMENTS = {
    "/blog/", "/nyheder/", "/news/",
    "/login", "/konto", "/min-side", "/kontakt",
    "/cookies", "/privatlivspolitik", "/handelsbetingelser",
    "/om-os", "/karriere", "/job",
    "/search", "/sog",
    "/category/", "/tag/",
    "/wp-content/", "/wp-admin/", "/wp-json/",
    ".pdf", ".jpg", ".png", ".css", ".js",
}


def is_likely_tour_url(operator: str, url: str) -> bool:
    """Test if a URL matches the tour-detail pattern for the given operator."""
    # Quick exclude
    url_lower = url.lower()
    if any(frag in url_lower for frag in EXCLUDE_FRAGMENTS):
        return False

    patterns = TOUR_URL_PATTERNS.get(operator)
    if not patterns:
        # Unknown operator — be permissive but warn caller.
        return False
    return any(p.match(url) for p in patterns)


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
    return [(u, _slug_from_url(u)) for u in tour_urls]


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
    return [(u, _slug_from_url(u)) for u in tour_urls]


# ---------------------------------------------------------------------------
# Top-level discover()
# ---------------------------------------------------------------------------

@dataclass
class DiscoveryResult:
    operator: str
    tours_found: list[tuple[str, str]]    # (url, slug)
    method_used: str                       # 'sitemap' | 'firecrawl-map' | 'failed'
    notes: str = ""


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
    # Strategy 1 — sitemap
    if sitemap_url:
        sitemap_results = discover_via_sitemap(operator, sitemap_url)
        if sitemap_results is not None and len(sitemap_results) >= 5:
            # Got a useful number of URLs from sitemap
            return DiscoveryResult(
                operator=operator,
                tours_found=sitemap_results,
                method_used="sitemap",
                notes=f"{len(sitemap_results)} tour-URLs from sitemap",
            )

    # Strategy 2 — Firecrawl /map
    map_results = discover_via_firecrawl_map(operator, homepage_url, firecrawl_client)
    if map_results:
        return DiscoveryResult(
            operator=operator,
            tours_found=map_results,
            method_used="firecrawl-map",
            notes=f"{len(map_results)} tour-URLs from Firecrawl map",
        )

    return DiscoveryResult(
        operator=operator,
        tours_found=[],
        method_used="failed",
        notes="Neither sitemap nor Firecrawl map yielded results",
    )


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
