"""
Sitemap-based variant discovery (lesson from the n8n-agent experiment).

Some operators expose multiple departures as separate URL-variants of the
same product page. Albatros's pattern:

    base:    https://www.albatros.dk/rejser/albatros-nepal
    variant: https://www.albatros.dk/rejser/albatros-nepal?variant=20261029
    variant: https://www.albatros.dk/rejser/albatros-nepal?variant=20270304

Each variant URL = one departure. The date is encoded in the variant param
(YYYYMMDD). The variants are listed in albatros.dk/sitemap.xml.

Firecrawl + LLM-extraction on the BASE URL only sees one departure (the
default selected one), because the page is React-rendered and the second
date isn't visible until you click. But sitemap.xml is server-side XML —
Firecrawl gets it instantly, and we can parse all variant URLs.

This module replicates that "agent technique" without needing an autonomous
agent. We know Albatros's pattern; we encode it directly. Cost: ~$0.005
extra per Albatros scrape (one sitemap fetch). Reliability: high — sitemaps
rarely change format.

Future operators with similar patterns can be added by extending the
OPERATOR_VARIANT_PATTERNS registry below.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Optional, Iterable
from urllib.parse import urlparse


# Per-operator variant-URL patterns. Each entry knows:
#   - sitemap_url: where to fetch the operator's URL listing
#   - variant_pattern: regex to find variant URLs and extract the date param
#   - extract_date: function that converts variant-param string to ISO date
OPERATOR_VARIANT_PATTERNS: dict = {
    "Albatros Travel": {
        "sitemap_url": "https://www.albatros.dk/sitemap.xml",
        # Matches URLs like albatros.dk/rejser/<slug>?variant=20261029
        # The capture group is YYYYMMDD
        "variant_pattern": re.compile(
            r"https://www\.albatros\.dk/rejser/[^?\s\"<>]+\?variant=(\d{8})"
        ),
        "extract_date": lambda variant_param: _parse_yyyymmdd(variant_param),
    },
    # Future operators can be added here. e.g. if Stjernegaard exposes
    # variants via /priser-og-datoer/ subpages with date IDs.
}


def discover_variants(operator: str, product_base_url: str, scrape_callable) -> list[dict]:
    """Look up an operator's sitemap and return per-departure variant info
    matching the given product base URL.

    Args:
        operator: name as used in TourTarget (e.g. "Albatros Travel").
        product_base_url: the canonical product URL (e.g.
            https://www.albatros.dk/rejser/albatros-nepal).
        scrape_callable: a function (url) -> ScrapeResult, typically the
            FirecrawlClient.scrape bound method. Lets us inject mocks in tests.

    Returns:
        List of dicts with keys: variant_id, start_date (ISO YYYY-MM-DD), url.
        Empty list if operator has no variant pattern, sitemap unavailable, or
        no matching variants found.
    """
    pattern_config = OPERATOR_VARIANT_PATTERNS.get(operator)
    if not pattern_config:
        return []

    sitemap_url = pattern_config["sitemap_url"]
    variant_pattern: re.Pattern = pattern_config["variant_pattern"]
    extract_date = pattern_config["extract_date"]

    # Fetch the sitemap. Firecrawl returns it as text/markdown for XML files.
    sitemap_result = scrape_callable(sitemap_url)
    if not sitemap_result.success:
        return []

    content = sitemap_result.markdown or sitemap_result.html or ""
    if not content:
        return []

    # Filter to only variants of THIS specific product. Albatros's sitemap
    # lists every product's variants; we only want the ones for this URL.
    product_path = urlparse(product_base_url).path  # /rejser/albatros-nepal

    variants: list[dict] = []
    seen_variants: set[str] = set()

    for match in variant_pattern.finditer(content):
        full_url = match.group(0)
        variant_id = match.group(1)

        # Only include variants whose URL path matches this product's path.
        # Without this filter we'd mix departures from every Albatros tour
        # in the sitemap.
        if urlparse(full_url).path != product_path:
            continue

        if variant_id in seen_variants:
            continue
        seen_variants.add(variant_id)

        iso_date = extract_date(variant_id)
        if not iso_date:
            continue

        variants.append({
            "variant_id": variant_id,
            "start_date": iso_date,
            "url": full_url,
        })

    variants.sort(key=lambda v: v["start_date"])
    return variants


def merge_variants_into_departures(
    base_departures: list[dict],
    variants: list[dict],
    from_price_dkk: Optional[int],
) -> list[dict]:
    """Combine per-departure data from base scrape with sitemap-discovered variants.

    Strategy:
      - Variants from sitemap are authoritative for which dates exist.
      - Base scrape may have richer per-departure data (price, status,
        rejseleder) for the date currently selected on the page.
      - For variant dates without base-scrape data, populate price from
        the page's from-price (which Albatros uses as the standard price
        across departures).

    Returns merged departures list, sorted chronologically, deduplicated by date.
    """
    # Index base departures by date for quick lookup
    base_by_date = {d["start_date"]: d for d in base_departures}

    merged: list[dict] = []
    for v in variants:
        existing = base_by_date.get(v["start_date"])
        if existing:
            # Use base-scrape data — it's richer (status, named rejseleder, etc.)
            merged.append(existing)
        elif from_price_dkk is not None:
            # Variant exists in sitemap but base scrape didn't see it.
            # Build a minimal departure record from variant + page from-price.
            merged.append({
                "departure_code": v["variant_id"],
                "start_date": v["start_date"],
                "end_date": None,
                "price_dkk": from_price_dkk,
                "availability_status": "Åben",
                "flight_origin": "København",
                "rejseleder_name": None,
            })
        else:
            # No base-scrape data and no from-price — can't construct a valid
            # departure record. Skip and let the analyst review.
            continue

    # Also include any base departures whose dates aren't in the sitemap
    # (rare, but possible — e.g. last-minute additions not yet in sitemap).
    variant_dates = {v["start_date"] for v in variants}
    for d in base_departures:
        if d["start_date"] not in variant_dates:
            merged.append(d)

    merged.sort(key=lambda d: d["start_date"])
    return merged


def _parse_yyyymmdd(s: str) -> Optional[str]:
    """Convert '20261029' to '2026-10-29'. Returns None if invalid."""
    if not isinstance(s, str) or len(s) != 8 or not s.isdigit():
        return None
    try:
        year = int(s[:4])
        month = int(s[4:6])
        day = int(s[6:8])
        return date(year, month, day).isoformat()
    except ValueError:
        return None
