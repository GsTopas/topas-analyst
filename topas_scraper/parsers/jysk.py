"""
Jysk Rejsebureau parser.

Jysk's eligible-Fællesrejse pool is thin — typically one published departure
at a time, with status "På forespørgsel" even when a date and price are both
visible. That's still eligible per taxonomy §2.bis (per-departure rule with
thin pair-pool produces low-confidence comparisons, not no comparison).

The departure table looks roughly like:

    Status              Rejseperiode             Udrejse              Rejseleder       Pris
    På forespørgsel     22.08.26 - 29.08.26      København            Thomas Lyhne     13.950,-
"""

from __future__ import annotations

import re
from typing import Optional

from .base import make_parsed_tour, parse_danish_date, parse_price, normalize_status, slugify


def parse(scrape, target):
    md = scrape.markdown or ""

    tour = make_parsed_tour(
        target,
        operator="Jysk Rejsebureau",
        duration_days=_extract_duration(md),
        from_price_dkk=_extract_from_price(md),
        eligibility_notes="Single-departure thin pool — confidence band drops to Low (§2.bis).",
    )

    departures = _extract_departures(md)
    return tour.to_dict(), departures


def _extract_duration(md: str) -> Optional[int]:
    m = re.search(r"Varighed\s*(\d+)\s*dage", md)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s+dage\b", md)
    return int(m.group(1)) if m else None


def _extract_from_price(md: str) -> Optional[int]:
    # "Frapris pr. pers. **Fra 13.950,-**"
    m = re.search(r"Fra\s+([\d.]+)", md, re.IGNORECASE)
    return parse_price(m.group(1)) if m else None


def _extract_departures(md: str) -> list[dict]:
    """Jysk departures: dd.mm.yy format with status and rejseleder name.

    Markdown table rows use | delimiters. We match a date range and look ahead for
    a price within the same row context.
    """
    departures = []
    seen = set()

    # Match: dd.mm.yy - dd.mm.yy ... up to ~400 chars ... price followed by ",-"
    pattern = re.compile(
        r"(\d{2}\.\d{2}\.\d{2,4})"                      # start date
        r"\s*[–\-]\s*"
        r"(\d{2}\.\d{2}\.\d{2,4})"                      # end date
        r"([\s\S]{0,500}?)"                              # intervening (table cells)
        r"([\d]{2,3}\.[\d]{3}),?\-?",                    # price like 13.950,-
        re.IGNORECASE
    )

    for m in pattern.finditer(md):
        start_str, end_str, between, price_str = m.group(1), m.group(2), m.group(3), m.group(4)
        start = parse_danish_date(start_str)
        end = parse_danish_date(end_str)
        if not start:
            continue
        key = start.isoformat()
        if key in seen:
            continue
        seen.add(key)

        # Look in 'between' first, then a small window before, for status
        head = md[max(0, m.start() - 200):m.start()] + between
        status_match = re.search(r"(På forespørgsel|Garanteret|Få pladser|Udsolgt|Åben for booking|Ledig)", head, re.IGNORECASE)
        status = normalize_status(status_match.group(1)) if status_match else "På forespørgsel"

        # Look for rejseleder name in the row
        rl_match = re.search(r"Rejseleder\s+([A-ZÆØÅ][a-zæøå]+(?:\s+[A-ZÆØÅ][a-zæøå]+)+)", between)
        rejseleder = rl_match.group(1) if rl_match else None

        departures.append({
            "departure_code": None,
            "start_date": start.isoformat(),
            "end_date": end.isoformat() if end else None,
            "price_dkk": parse_price(price_str),
            "availability_status": status,
            "flight_origin": "København",
            "rejseleder_name": rejseleder,
        })

    departures.sort(key=lambda d: d["start_date"])
    return departures
