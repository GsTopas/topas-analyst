"""
Best Travel parser.

Best Travel (Stena Line Travel Group) is a Danish operator focused on
kulturrejser med dansk rejseleder for det modne publikum/seniorer. Their
Madeira product is a cultural tour with bus-based excursions, NOT a
vandreferie — methodologically a Tier 3 competitor for PTMD (same destination
+ Fællesrejse format, different product type).

Best Travel uses dynamic JavaScript rendering for product listings ("Loading..."
in static HTML), but the per-tour pages have visible departure data when the
page renders. This parser is best-effort — if departures don't extract,
the tour is still recorded as eligible-but-divergent.
"""

from __future__ import annotations

import re
from typing import Optional

from .base import make_parsed_tour, parse_danish_date, parse_price, normalize_status, slugify


def parse(scrape, target):
    md = scrape.markdown or ""

    tour = make_parsed_tour(
        target,
        operator="Best Travel",
        duration_days=_extract_duration(md),
        from_price_dkk=_extract_from_price(md),
        eligibility_notes=(
            "Stena Line Travel Group brand. Kultur-orienteret. "
            "For PTMD: T3 — samme Fællesrejse-format, men kulturrejse med "
            "bus-udflugter, ikke vandreferie. Anses som ineligible for direct "
            "pris-sammenligning på PTMD."
        ),
    )

    departures = _extract_departures(md)
    return tour.to_dict(), departures


def _extract_duration(md: str) -> Optional[int]:
    m = re.search(r"(\d+)\s+dage?s?\b", md, re.IGNORECASE)
    return int(m.group(1)) if m else None


def _extract_from_price(md: str) -> Optional[int]:
    patterns = [
        r"pris\s+pr\.?\s+person\s+fra[\s\S]{0,40}?([\d.]+)",
        r"pris\s+fra[\s\S]{0,40}?([\d.]+)\s*kr",
        r"fra\s+kr\.?\s*([\d.]+)",
        r"fra\s+([\d.]+)\s*kr",
    ]
    for pat in patterns:
        m = re.search(pat, md, re.IGNORECASE)
        if m:
            price = parse_price(m.group(1))
            if price and price > 5000:
                return price

    prices = [parse_price(p) for p in re.findall(r"([\d.]+)\s*kr\b", md, re.IGNORECASE)]
    prices = [p for p in prices if p and p > 5000]
    return min(prices) if prices else None


def _extract_departures(md: str) -> list[dict]:
    """Best Travel: Loading-only listings often. Best-effort extraction."""
    departures = []
    seen = set()

    pattern = re.compile(
        r"(\d{1,2}\.\s+[a-zæøå]+\s+\d{4})"
        r"[\s\S]{0,100}?([\d.]+)\s*kr",
        re.IGNORECASE,
    )
    for m in pattern.finditer(md):
        date_str, price_str = m.group(1), m.group(2)
        start = parse_danish_date(date_str)
        if not start:
            continue
        price = parse_price(price_str)
        if not price or price < 5000:
            continue
        key = start.isoformat()
        if key in seen:
            continue
        seen.add(key)
        departures.append({
            "departure_code": None,
            "start_date": start.isoformat(),
            "end_date": None,
            "price_dkk": price,
            "availability_status": "Åben",
            "flight_origin": "Aalborg",  # Best Travel kører fra Aalborg
            "rejseleder_name": None,
        })

    departures.sort(key=lambda d: d["start_date"])
    return departures
