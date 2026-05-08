"""
Viktors Farmor parser.

Per taxonomy section 2.12: Viktors Farmor renders departures client-side via
JavaScript. Firecrawl's JS rendering captures them. The structure is a block
per departure starting with a bold date line of the form ``**9. maj 26**``
(sometimes the day-number dot is markdown-escaped in the raw output).

Each departure block contains: date, "8 dage", price in kr., a rejseleder
link, status text, airport, and a Bestil/Forespoerg link.

Strategy: find each bold-wrapped abbreviated date, then read forward through
the next ~500 chars for that departure's attributes. The parser captures
named rejseledere as a quality signal per taxonomy section 2.7.
"""

from __future__ import annotations

import re
from typing import Optional

from .base import make_parsed_tour, parse_danish_date, parse_price, normalize_status, slugify


def parse(scrape, target):
    md = scrape.markdown or ""

    departures = _extract_departures(md)

    eligibility_notes = "Tier 1 anchor — JS-rendered departures captured."
    if not departures:
        eligibility_notes = "Eligibility uncertain — no departures parsed (taxonomy section 2.12)."

    tour = make_parsed_tour(
        target,
        operator="Viktors Farmor",
        duration_days=_extract_duration(md),
        from_price_dkk=_extract_from_price(md),
        eligibility_notes=eligibility_notes,
    )

    return tour.to_dict(), departures


def _extract_duration(md: str) -> Optional[int]:
    m = re.search(r"(\d+)\s+dages?\s+rejse", md, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(\d+)\s+dage\b", md)
    return int(m.group(1)) if m else None


def _extract_from_price(md: str) -> Optional[int]:
    # Prefer the "Priser fra ... 13.990 kr." pattern in the head of the page
    m = re.search(r"Priser?\s+fra[\s\S]{0,40}?([\d.]+)\s*kr", md, re.IGNORECASE)
    if m:
        return parse_price(m.group(1))
    # Fallback to the smallest price found on the page
    prices = [parse_price(p) for p in re.findall(r"([\d.]+)\s*kr\.", md)]
    prices = [p for p in prices if p and p > 5000]
    return min(prices) if prices else None


# Danish month abbreviations seen in VF dates: "jan", "feb", "mar", "apr", "maj",
# "jun", "jul", "jul.", "aug", "sep", "okt", "nov", "dec". Some have a trailing dot.
MONTH_ABBR_PATTERN = r"(?:jan|feb|mar|apr|maj|jun|jul|aug|sep|okt|nov|dec)\.?"


def _extract_departures(md: str) -> list[dict]:
    """Find each "**N. month YY**" bold-marked date anchor and read forward."""
    departures = []
    seen = set()

    # Anchor: bold-wrapped date with 2-digit year, e.g. "**9\\. maj 26**" or "**18\\. jul. 26**".
    # The escape backslash before the dot is sometimes present in markdown output.
    anchor_re = re.compile(
        r"\*\*\s*(\d{1,2})\\?\.\s+(" + MONTH_ABBR_PATTERN + r")\s+(\d{2})\s*\*\*",
        re.IGNORECASE
    )

    for m in anchor_re.finditer(md):
        day, month_abbr, year2 = m.group(1), m.group(2), m.group(3)
        month_abbr = month_abbr.rstrip(".").lower()
        # Reuse parse_danish_date by building a normalized "DD. month YYYY" string
        date_str = f"{day}. {month_abbr} 20{year2}"
        start = parse_danish_date(date_str)
        if not start:
            continue
        if start.isoformat() in seen:
            continue
        seen.add(start.isoformat())

        # Read forward 500 chars for price, rejseleder, status, airport
        tail = md[m.end():m.end() + 500]

        price_match = re.search(r"([\d.]+)\s*kr\.", tail)
        price = parse_price(price_match.group(1)) if price_match else None

        # Rejseleder appears as a markdown link with the name in the visible text
        rl_match = re.search(
            r"\]\((?:https?://)?[^)]*?/rejseledere/[^)]+\)",
            tail
        )
        if rl_match:
            # Walk back from the link to find the visible name
            link_text_match = re.search(r"\\\n([A-ZÆØÅ][^\]]{2,40})\]", tail[:rl_match.end()])
            rejseleder = link_text_match.group(1).strip() if link_text_match else None
        else:
            rejseleder = None

        status_match = re.search(
            r"\b(Udsolgt|Få pladser|Faa pladser|Garanteret|Ledig|Afventer pris)\b",
            tail,
            re.IGNORECASE
        )
        status = normalize_status(status_match.group(1)) if status_match else "Ukendt"

        airport_match = re.search(r"\b(København|Aalborg|Billund)\b", tail)
        airport = airport_match.group(1) if airport_match else "København"

        departures.append({
            "departure_code": None,
            "start_date": start.isoformat(),
            "end_date": None,
            "price_dkk": price,
            "availability_status": status,
            "flight_origin": airport,
            "rejseleder_name": rejseleder,
        })

    departures.sort(key=lambda d: d["start_date"])
    return departures
