"""
Ruby Rejser parser.

Ruby's product page is cookie-walled and client-side rendered (handled in
config.py via scrape_overrides: only_main_content=False, wait_for=3000).

Once rendered, the page exposes:

- Tour name in H1
- "Travel Code: VAG-053" — used as tour_code-equivalent
- Sværhedsgrad in H2 ("Sværhedsgrad 2-3")
- Departure data inside a markdown table with rows like:
    "#### Uge 48: Startdato 21.11.2026 varighed 7 nætter | Fra 12.998,- DKK"
- Per-værelse pricing inside the table:
    "1 person Enkeltværelse | 14.998,- DKK | [Bestil](...)"
    "2 personer Dobbeltværelse | 12.998,- DKK | [Bestil](...)"
- Rejseleder named in "Turleder: Karin Svane" line.

Pricing convention: we use the per-person *Dobbeltværelse* price as the
canonical departure price, since that matches Topas's "basis dobbeltværelse"
convention from methodology section 5.1. Single-room is a tillæg, not the
headline price.

The price format is "12.998,- DKK" (Danish thousand-separator dot, comma-dash,
DKK suffix). parse_price() handles this once we strip ",-".
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Optional

from .base import make_parsed_tour, parse_danish_date, parse_price, normalize_status, slugify


def parse(scrape, target):
    md = scrape.markdown or ""

    tour = make_parsed_tour(
        target,
        operator="Ruby Rejser",
        tour_code=_extract_travel_code(md),  # Override target.tour_code with Ruby's VAG-NNN
        duration_days=_extract_duration(md),
        from_price_dkk=_extract_from_price(md),
        eligibility_notes="Small portfolio. Per-person dobbeltværelse-pris used as canonical price (methodology §5.1).",
    )

    departures = _extract_departures(md)
    return tour.to_dict(), departures


def _extract_travel_code(md: str) -> Optional[str]:
    # Ruby uses internal codes like "VAG-053"
    m = re.search(r"Travel\s+Code\W+([A-Z]{2,4}-\d{3})", md, re.IGNORECASE)
    return m.group(1) if m else None


def _extract_duration(md: str) -> Optional[int]:
    # Page uses "7 nætter" (sleeping nights) — duration_days = nights + 1.
    # If we see "varighed N nætter", use that. Else look for "N dage".
    m = re.search(r"varighed\s+(\d+)\s+nætter", md, re.IGNORECASE)
    if m:
        return int(m.group(1)) + 1  # 7 nætter = 8 dage
    m = re.search(r"(\d+)\s+nætter", md)
    if m:
        return int(m.group(1)) + 1
    m = re.search(r"(\d+)\s+dage\b", md)
    return int(m.group(1)) if m else None


def _extract_from_price(md: str) -> Optional[int]:
    # Ruby format: "Fra 12.998,- DKK" — comma-dash before DKK is the giveaway.
    # Multiple prices appear; we want the page-level "Fra X,- DKK" headline.
    m = re.search(r"Fra\s+([\d.]+),\-?\s*DKK", md)
    if m:
        return parse_price(m.group(1))
    # Fallback: smallest plausible price on the page.
    prices = [parse_price(p) for p in re.findall(r"([\d.]+),\-?\s*DKK", md)]
    prices = [p for p in prices if p and p > 5000]
    return min(prices) if prices else None


def _extract_rejseleder(md: str) -> Optional[str]:
    # "**Turleder:**Karin Svane" or "Turleder: Karin Svane"
    m = re.search(r"Turleder\W+([A-ZÆØÅ][a-zæøå]+(?:\s+[A-ZÆØÅ][a-zæøå]+)+)", md)
    return m.group(1).strip() if m else None


def _extract_departures(md: str) -> list[dict]:
    """Parse departure rows from Ruby's markdown table.

    The pattern we care about looks like:
        #### Uge 48: Startdato 21.11.2026 varighed 7 nætter | Fra 12.998,- DKK

    Then within the same departure block, room-type rows give per-person prices:
        2 personer Dobbeltværelse | 12.998,- DKK | [Bestil](...)

    We use the Dobbeltværelse per-person price as the canonical price.
    """
    departures = []
    seen = set()

    rejseleder = _extract_rejseleder(md)

    # Anchor on "Startdato dd.mm.yyyy"
    anchor_re = re.compile(
        r"Startdato\s+(\d{1,2}\.\d{1,2}\.\d{4})\s+varighed\s+(\d+)\s+nætter",
        re.IGNORECASE
    )

    for m in anchor_re.finditer(md):
        date_str, nights = m.group(1), int(m.group(2))
        # Convert dd.mm.yyyy to ISO
        try:
            day, month, year = (int(x) for x in date_str.split("."))
            start = date(year, month, day)
        except (ValueError, OverflowError):
            continue

        key = start.isoformat()
        if key in seen:
            continue
        seen.add(key)

        # Look in the next 1500 chars for the Dobbeltværelse price (canonical).
        # Fall back to "Fra X,- DKK" near the anchor if room-type table isn't there.
        tail = md[m.start():m.start() + 1500]

        # Prefer 2-person Dobbeltværelse (basis dobbeltværelse per methodology §5.1)
        dobbel_match = re.search(
            r"2\s+personer\s+Dobbeltv[æa]relse\s*\|\s*([\d.]+),\-?\s*DKK",
            tail,
            re.IGNORECASE
        )
        if dobbel_match:
            price = parse_price(dobbel_match.group(1))
        else:
            fra_match = re.search(r"Fra\s+([\d.]+),\-?\s*DKK", tail)
            price = parse_price(fra_match.group(1)) if fra_match else None

        end = start + timedelta(days=nights)

        departures.append({
            "departure_code": None,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "price_dkk": price,
            "availability_status": "Åben",  # Page shows "In stock" + active Bestil buttons
            "flight_origin": "København",   # Also AAB available; CPH is canonical
            "rejseleder_name": rejseleder,
        })

    departures.sort(key=lambda d: d["start_date"])
    return departures
