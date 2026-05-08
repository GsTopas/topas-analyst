"""
Albatros Travel parser.

Albatros uses heavy client-side rendering. Their product pages show data
as JS placeholders before render:
  - "13 [days]" instead of "13 dage" (i18n placeholder)
  - "[[seeAvailableDates]]" in place of departure data
  - Headline price (e.g. "17.998 kr.") IS visible server-side

What we can extract reliably:
  - tour_name, country, region (from target config)
  - duration_days (from "13 [days]" or "13 dage" headline)
  - from_price_dkk (from "X.XXX kr." in header)

What we CAN'T extract without full JS rendering:
  - per-departure dates and prices (rendered behind seeAvailableDates click)

For methodology purposes this means Albatros tours are recorded as eligible
Tier 2 with a documented from-price, but per-departure pair-comparison is
not possible until rendering improves. Marked clearly in eligibility_notes
so the UI can distinguish "data limitation" from "product ineligible".
"""

from __future__ import annotations

import re
from typing import Optional

from .base import make_parsed_tour, parse_danish_date, parse_price, normalize_status


def parse(scrape, target):
    md = scrape.markdown or ""

    duration = _extract_duration(md)
    from_price = _extract_from_price(md)
    departures = _extract_departures(md)

    # Build eligibility notes based on what we got
    if departures:
        notes = "Albatros Travel — store grupperejser med dansk rejseleder."
        eligible = True
    elif from_price and duration:
        # We have headline data but no departures — Albatros's JS-render limitation
        notes = (
            f"Albatros Travel — {duration} dage fra {from_price:,} kr. "
            "Per-departure data ikke tilgængelig (kræver fuld JS-render). "
            "Headline-pris brugt som T2 reference-punkt."
        ).replace(",", ".")
        eligible = True
    else:
        # Empty page — Albatros has no product at this URL or scrape failed
        notes = (
            "Ingen data udtrukket. Mulige årsager: (1) Albatros har intet produkt "
            "på denne destination, (2) JS-render fejlede, (3) URL forældet."
        )
        eligible = False

    tour = make_parsed_tour(
        target,
        operator="Albatros Travel",
        duration_days=duration,
        from_price_dkk=from_price,
        fællesrejse_eligible=eligible,
        eligibility_notes=notes,
    )

    return tour.to_dict(), departures


def _extract_duration(md: str) -> Optional[int]:
    """Match either '13 dage' or '13 [days]' (Albatros JS placeholder)."""
    patterns = [
        r"(\d+)\s+dage\b",          # rendered Danish
        r"(\d+)\s+\[days\]",        # JS placeholder before render
        r"(\d+)\s+days\b",          # English variant
    ]
    for pat in patterns:
        m = re.search(pat, md, re.IGNORECASE)
        if m:
            return int(m.group(1))
    return None


def _extract_from_price(md: str) -> Optional[int]:
    """Albatros header shows: 'X.XXX kr.' — number BEFORE 'kr.'."""
    # Look in first 5000 chars (header area) to avoid picking up tilkøb prices
    head = md[:5000]
    m = re.search(r"([\d.]+)\s*kr\.", head, re.IGNORECASE)
    if m:
        price = parse_price(m.group(1))
        if price and price > 5000:
            return price

    # Fallback: any kr-suffixed price >5000
    prices = [parse_price(p) for p in re.findall(r"([\d.]+)\s*kr\b", md, re.IGNORECASE)]
    prices = [p for p in prices if p and p > 5000]
    return min(prices) if prices else None


def _extract_departures(md: str) -> list[dict]:
    """Try to extract departures if Firecrawl rendered them.

    Albatros's rendered format (when JS works):
      | Dato | Pris | Status |
      | 2025-02-04 | 20.998 kr. | ... |

    Or as ISO dates: '2025-02-04T00:00:00+00:00 · Pris · 20.998 kr.'
    """
    departures = []
    seen = set()

    # Pattern 1: ISO date · Pris · NN.NNN kr.
    pattern_iso = re.compile(
        r"(\d{4}-\d{2}-\d{2})"
        r"[\s\S]{0,80}?"
        r"([\d.]+)\s*kr",
        re.IGNORECASE,
    )
    for m in pattern_iso.finditer(md):
        date_str = m.group(1)
        price_str = m.group(2)
        try:
            from datetime import date
            year, month, day = date_str.split("-")
            start = date(int(year), int(month), int(day))
        except (ValueError, AttributeError):
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
            "flight_origin": "København",
            "rejseleder_name": None,
        })

    # Pattern 2: Danish date "DD. mmm. YYYY" with price
    pattern_da = re.compile(
        r"(\d{1,2}\.\s+[a-zæøå]+\.?\s+\d{4})"
        r"[\s\S]{0,80}?"
        r"([\d.]+)\s*kr",
        re.IGNORECASE,
    )
    for m in pattern_da.finditer(md):
        date_str = m.group(1)
        price_str = m.group(2)
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
            "flight_origin": "København",
            "rejseleder_name": None,
        })

    departures.sort(key=lambda d: d["start_date"])
    return departures

