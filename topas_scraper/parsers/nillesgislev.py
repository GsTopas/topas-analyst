"""
Nilles & Gislev parser.

Aller Leisure brand. Their Madeira product is sightseeing-led with optional
levada-vandring as tilkoeb -- different audience than PTMD. Worth tracking
because Aller portfolio overlap matters at holding level (taxonomy section 2.11).

OBSERVED at first scrape (run 2ac38c06): the page shows
"Nye afgange paa vej" in both the duration and pris-fra slots. There are no
published departures. Per taxonomy section 2.3, this fails fællesrejse-eligibility
criterion 1 (fixed departure date). The tour is recorded as ineligible
rather than fabricating departure data from past references in body text.

When N&G publishes their next afgange (next year cycle), this parser will
need to re-validate. Expected structure when populated: per-departure table
with date + airport + price + status, similar to Smilrejser.
"""

from __future__ import annotations

import re
from typing import Optional

from .base import make_parsed_tour, parse_danish_date, parse_price, normalize_status, slugify


def parse(scrape, target):
    md = scrape.markdown or ""

    # Eligibility check: detect "Nye afgange paa vej" or "Nye afgange på vej"
    no_departures_signal = re.search(r"Nye\s+afgange\s+p[aå]\s+vej", md, re.IGNORECASE)

    duration = _extract_duration(md)
    from_price = _extract_from_price(md)
    departures = _extract_departures(md) if not no_departures_signal else []

    if no_departures_signal or not departures:
        eligibility = False
        notes = "INELIGIBLE — page shows 'Nye afgange paa vej', no published afgange. Aller Leisure brand."
    else:
        eligibility = True
        notes = "Aller Leisure brand — Tier 2 for PTMD (sightseeing-led, levada-vandring as tilkoeb)."

    tour = make_parsed_tour(
        target,
        operator="Nilles & Gislev",
        duration_days=duration,
        from_price_dkk=from_price,
        fællesrejse_eligible=eligibility,
        eligibility_notes=notes,
    )

    return tour.to_dict(), departures


def _extract_duration(md: str) -> Optional[int]:
    # Look for "X dage" but skip if "Nye afgange paa vej" precedes it
    if re.search(r"Nye\s+afgange\s+p[aå]\s+vej", md, re.IGNORECASE):
        # No published afgange — duration not knowable from body alone
        # but fall through to look for "X dages rejse" or similar generic copy
        m = re.search(r"(\d+)\s+dages?\s+rejse", md, re.IGNORECASE)
        return int(m.group(1)) if m else None
    m = re.search(r"\b(\d+)\s+dage\b", md, re.IGNORECASE)
    return int(m.group(1)) if m else None


def _extract_from_price(md: str) -> Optional[int]:
    # If "Nye afgange paa vej" is in pris-fra slot, return None.
    # Look for "fra X kr" pattern that's NOT preceded by "Nye afgange".
    pris_fra_pattern = re.compile(
        r"\*\*Pris\s+fra\*\*[\s\S]{0,80}?(Nye\s+afgange|([\d.]+)\s*kr)",
        re.IGNORECASE
    )
    m = pris_fra_pattern.search(md)
    if m and m.group(2):
        return parse_price(m.group(2))
    # Fallback: smallest plausible price on the page (filter out tilkoeb prices like 350-450 kr)
    prices = [parse_price(p) for p in re.findall(r"([\d.]+)\s*kr\.", md)]
    prices = [p for p in prices if p and p > 5000]
    return min(prices) if prices else None


def _extract_departures(md: str) -> list[dict]:
    """Look for per-departure table rows. Currently empty for Madeira but kept
    so when N&G publishes new afgange the structure is ready.
    """
    departures = []
    seen = set()

    # Pattern: a date with airport + price somewhere nearby. This is intentionally
    # broad because we don't yet have a populated example to tune against.
    pattern = re.compile(
        r"(\d{1,2}\.?\s*[a-zæøå]+\s+\d{4}|\d{2}[/.]\d{2}[/.]\d{2,4})"
        r"[\s\S]{0,200}?"
        r"(København|Aalborg|Billund)?"
        r"[\s\S]{0,80}?"
        r"([\d.]+)\s*kr\.",
        re.IGNORECASE
    )

    for m in pattern.finditer(md):
        date_str, airport, price_str = m.group(1), m.group(2), m.group(3)
        start = parse_danish_date(date_str)
        if not start:
            continue
        # Filter out junk dates (page-fixed dates like 2025 references)
        if start.year < 2026:
            continue
        price = parse_price(price_str)
        if not price or price < 5000:
            continue
        key = (start.isoformat(), airport or "København")
        if key in seen:
            continue
        seen.add(key)

        tail = md[m.end():m.end() + 200]
        status_match = re.search(r"\b(Garanteret|Få pladser|Udsolgt|Åben|Ledig)\b", tail, re.IGNORECASE)
        status = normalize_status(status_match.group(1)) if status_match else "Åben"

        departures.append({
            "departure_code": None,
            "start_date": start.isoformat(),
            "end_date": None,
            "price_dkk": price,
            "availability_status": status,
            "flight_origin": airport or "København",
            "rejseleder_name": None,
        })

    departures.sort(key=lambda d: d["start_date"])
    return departures
