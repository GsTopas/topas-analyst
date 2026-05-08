"""
Smilrejser parser.

Smilrejser exposes per-departure data cleanly with afrejsedato + afrejsested + price + status
all visible. Each departure block looks roughly like:

    16.05.2026   8 dage   København   12.995 DKK   Udsolgt
    29.07.2026   8 dage   København   12.995 DKK   +8 pladser

The status "+8 pladser" is normalized to "Åben" via base.STATUS_MAP.
The "Udsolgt" status is preserved — taxonomy v0.6 §13.2 treats this as a real demand signal.
"""

from __future__ import annotations

import re
from typing import Optional

from .base import make_parsed_tour, parse_danish_date, parse_price, normalize_status, slugify


def parse(scrape, target):
    md = scrape.markdown or ""

    tour = make_parsed_tour(
        target,
        operator="Smilrejser",
        duration_days=_extract_duration(md),
        from_price_dkk=_extract_from_price(md),
        eligibility_notes="Aller Leisure brand — Tier 1 anchor for PTMD.",
    )

    departures = _extract_departures(md)
    return tour.to_dict(), departures


def _extract_duration(md: str) -> Optional[int]:
    # "8 dages vandreferie" or "8 dage"
    m = re.search(r"(\d+)\s+dage?s?\b", md, re.IGNORECASE)
    return int(m.group(1)) if m else None


def _extract_from_price(md: str) -> Optional[int]:
    # "Pris pr. person fra ... 12.995 DKK"
    m = re.search(r"pris\s+pr\.?\s+person\s+fra[\s\S]{0,80}?([\d.]+)\s*DKK", md, re.IGNORECASE)
    if m:
        return parse_price(m.group(1))
    # Fallback: smallest DKK figure on the page
    prices = [parse_price(p) for p in re.findall(r"([\d.]+)\s*DKK", md)]
    prices = [p for p in prices if p and p > 5000]
    return min(prices) if prices else None


def _extract_departures(md: str) -> list[dict]:
    """Smilrejser departures use dd.mm.yyyy format and have airport, price, status visible."""
    departures = []
    seen = set()

    # Pattern: dd.mm.yyyy   N dage   Airport   N.NNN DKK   StatusText
    pattern = re.compile(
        r"(\d{2}\.\d{2}\.\d{4})"                  # date
        r"\s+\d+\s+dage"                          # duration
        r"\s+(København|Aalborg|Billund)"         # airport
        r"\s+([\d.]+)\s*DKK"                      # price
        r"\s+(Udsolgt|\+\d+\s*pladser|Få pladser|Garanteret)",  # status
        re.IGNORECASE
    )

    for m in pattern.finditer(md):
        date_str, airport, price_str, status_str = m.group(1), m.group(2), m.group(3), m.group(4)
        start = parse_danish_date(date_str)
        if not start:
            continue
        key = (start.isoformat(), airport)
        if key in seen:
            continue
        seen.add(key)

        departures.append({
            "departure_code": None,
            "start_date": start.isoformat(),
            "end_date": None,
            "price_dkk": parse_price(price_str),
            "availability_status": normalize_status(status_str),
            "flight_origin": airport,
            "rejseleder_name": None,
        })

    departures.sort(key=lambda d: d["start_date"])
    return departures
