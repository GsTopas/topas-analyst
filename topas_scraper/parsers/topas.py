"""
Topas parser.

Topas's Firecrawl-rendered markdown structures each departure across multiple
lines (the day number is followed by an escaped dot like ``10\\. juli 2026``):

    10. juli 2026
    --
    17. juli 2026

    Faa pladser
    13.970 DKK
    [Bestil Rejse](https://www.topas.dk/pages/checkout?tripDesignator=PTMD&tripCode=PTMD2605&...)

The dates, separator, status, and price are each on their own lines. We use
the tripCode in the Bestil link as a stable departure_code.

Strategy: locate each `tripCode=...` link first, then look BACKWARDS to find
the date, status, and price for that departure. This is more robust than trying
to match a free-form date+separator+date pattern across multiple lines.
"""

from __future__ import annotations

import re
from typing import Optional

from .base import make_parsed_tour, parse_danish_date, parse_price, normalize_status, slugify


def parse(scrape, target):
    md = scrape.markdown or ""

    tour = make_parsed_tour(
        target,
        operator="Topas",
        duration_days=_extract_duration(md),
        from_price_dkk=_extract_from_price(md),
        eligibility_notes="Reference Fællesrejse — passes all 5 criteria.",
    )

    departures = _extract_departures(md)
    return tour.to_dict(), departures


def _extract_duration(md: str) -> Optional[int]:
    """Extract trip duration in days.

    Topas's headline format on tour pages is typically "X dage" (singular,
    no 's'). Other Topas tours mentioned on the same page (cross-promo)
    often use "X dages aktiv ferie..." — we want to skip those.

    Strategy:
      1. Prefer "X dage" NOT followed by 's' (most reliable headline pattern)
      2. Look for "Varighed" near a number
      3. Fallback: any "X dage(s)"
    """
    # Pattern 1: "X dage" without trailing 's' — exact headline format
    m = re.search(r"(\d+)\s+dage(?!s)", md, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # Pattern 2: "Varighed: X" or "Varighed X dage"
    m = re.search(r"Varighed[:\s]+(\d+)", md, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # Pattern 3: any "X dage(s)" as last resort
    m = re.search(r"(\d+)\s+dages?\b", md, re.IGNORECASE)
    return int(m.group(1)) if m else None


def _extract_from_price(md: str) -> Optional[int]:
    # "8 dage fra ... 9.970 DKK" — earliest fra-pris
    m = re.search(r"fra\s+\D{0,30}([\d.]+)\s*DKK", md, re.IGNORECASE)
    if m:
        return parse_price(m.group(1))
    # Fallback: smallest plausible DKK number
    prices = [parse_price(p) for p in re.findall(r"([\d.]+)\s*DKK", md)]
    prices = [p for p in prices if p and p > 5000]
    return min(prices) if prices else None


def _extract_departures(md: str) -> list[dict]:
    """Anchor on tripCode, then walk backwards from each one to find date+status+price.

    Each tripCode link looks like:
        [Bestil Rejse](https://www.topas.dk/pages/checkout?tripDesignator=PTMD&tripCode=PTMD2605&tripMainCategory=2)

    The 800 chars before each link reliably contain that departure's data.
    """
    departures = []
    seen = set()

    trip_pattern = re.compile(r"tripCode=([A-Z]{4}\d{4})")

    for trip_match in trip_pattern.finditer(md):
        trip_code = trip_match.group(1)
        if trip_code in seen:
            continue
        seen.add(trip_code)

        # Look in the 800 chars BEFORE this tripCode link for the dates, status, and price
        head_start = max(0, trip_match.start() - 800)
        head = md[head_start:trip_match.start()]

        # Find all dates in this window — the LAST two are this departure's start/end
        # (earlier dates belong to the previous departure block).
        # Format: "10\. juli 2026" with optional escape backslash.
        date_re = re.compile(r"(\d{1,2})\\?\.\s+([a-zæøå]+)\s+(\d{4})", re.IGNORECASE)
        date_matches = list(date_re.finditer(head))
        if len(date_matches) < 2:
            continue

        # Take the last 2 — they're the start/end for this departure
        start_match = date_matches[-2]
        end_match = date_matches[-1]
        start_str = f"{start_match.group(1)}. {start_match.group(2)} {start_match.group(3)}"
        end_str = f"{end_match.group(1)}. {end_match.group(2)} {end_match.group(3)}"
        start = parse_danish_date(start_str)
        end = parse_danish_date(end_str)
        if not start:
            continue

        # Find the price closest to the link (last price in head)
        price_matches = list(re.finditer(r"([\d.]+)\s*DKK", head))
        price = parse_price(price_matches[-1].group(1)) if price_matches else None

        # Find the status between the end-date and the link
        tail_after_dates = head[end_match.end():]
        status_match = re.search(
            r"(Garanteret afgang|Få pladser|Åben for booking|Udsolgt|Afventer pris|Ledig)",
            tail_after_dates,
            re.IGNORECASE
        )
        status = normalize_status(status_match.group(1)) if status_match else "Ukendt"

        departures.append({
            "departure_code": trip_code,
            "start_date": start.isoformat(),
            "end_date": end.isoformat() if end else None,
            "price_dkk": price,
            "availability_status": status,
            "flight_origin": "København",
            "rejseleder_name": None,
        })

    departures.sort(key=lambda d: d["start_date"])
    return departures
