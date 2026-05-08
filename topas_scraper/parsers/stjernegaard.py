"""
Stjernegaard parser.

Stjernegaard is part of Aller Leisure A/S. Their pricing/dates appear on the
main product page in an "## Andre spændende rejser" (Other related trips)
section — and the FIRST card in that section is always the focused product
itself (their CMS lists each product as its own first "related" item).

The structure when Firecrawl renders the page:

    ## Andre spændende rejser til <region>

    ### [<focused product name>](url) <country> Rundrejse med dansk rejseleder

    [<X> dage fra **<from-price> kr.**](url)

    | Afrejsedato |        |             |
    | ---         | ---    | ---         |
    | 14. mar. 2027 |      | kr. 24.990  |
    | 16. okt. 2026 | kr. 28.495 | Garanteret afgang |
    | 03. nov. 2026 | kr. 43.885 | 2 pladser tilbage |

    ### [<another product>](url) ...     ← NEXT card boundary

We MUST scope parsing to ONLY the first card to avoid pulling in departures
from related products in the same section. The boundary is the next "### ["
heading.

Format quirks within a card:
  - Danish month names ("14. mar. 2027"), NOT dd.mm.yyyy
  - Price prefixed with "kr." (kr BEFORE the number, opposite of most operators)
  - Status text varies: "Udsolgt", "Garanteret afgang", "X pladser tilbage",
    or empty (no status cell)
"""

from __future__ import annotations

import re
from typing import Optional

from .base import make_parsed_tour, parse_danish_date, parse_price, normalize_status


def parse(scrape, target):
    md = scrape.markdown or ""

    # SCOPE: focus on the first product card in the "Andre" section. Without
    # this scoping the parser would also capture sibling products' departures.
    md_focused = _scope_to_first_card(md)

    departures = _extract_departures(md_focused)

    notes = "Aller Leisure brand. Rundrejse med dansk rejseleder — Fællesrejse-eligible."
    if not departures:
        notes = (
            "No departures parsed. Check Firecrawl output for this URL — "
            "expected '## Andre spændende rejser' section with a date+price table."
        )

    tour = make_parsed_tour(
        target,
        operator="Stjernegaard Rejser",
        duration_days=_extract_duration(md),  # use full md for duration headline
        from_price_dkk=_extract_from_price(md),
        eligibility_notes=notes,
    )

    return tour.to_dict(), departures


def _scope_to_first_card(md: str) -> str:
    """Return the markdown subset containing only the first product card.

    Strategy:
      1. Find "## Andre spændende rejser" heading (start of related-products section)
      2. From there, find the FIRST "### [" heading (start of focused product card)
      3. From there, find the SECOND "### [" heading (start of next product card)
      4. Return everything between (1)→(3)

    If "Andre" heading isn't found, scan the whole markdown for the first
    "### [" → second "### [" pair as a fallback.
    """
    # Step 1: locate "Andre spændende" or just "## Andre"
    andre = re.search(r"##\s+Andre\b", md, re.IGNORECASE)
    body = md[andre.end():] if andre else md

    # Step 2: find first "### [" — start of focused card
    card_starts = list(re.finditer(r"###\s+\[", body))
    if not card_starts:
        return body  # no card structure — fall through

    first_card_start = card_starts[0].start()

    # Step 3: find second "### [" — start of next card (the boundary)
    if len(card_starts) >= 2:
        first_card_end = card_starts[1].start()
    else:
        # Only one card in the body — use rest of markdown
        first_card_end = len(body)

    return body[first_card_start:first_card_end]


def _extract_duration(md: str) -> Optional[int]:
    """Extract duration in days. Stjernegaard uses '14 dage', '15 dages rundrejse', etc."""
    # Prefer "X dage fra..." (their headline format)
    m = re.search(r"(\d+)\s+dage\s+fra\b", md, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # Generic fallback
    m = re.search(r"(\d+)\s+dage?s?\b", md, re.IGNORECASE)
    return int(m.group(1)) if m else None


def _extract_from_price(md: str) -> Optional[int]:
    """Stjernegaard: 'fra X.XXX kr.' — number BEFORE 'kr.', often with markdown bold."""
    patterns = [
        # "14 dage fra **24.990 kr.**" — number BEFORE kr. (most common)
        r"fra\s+\*{0,2}([\d.]+)\s*\*{0,2}\s*kr\b",
        # "Pris fra kr. 24.990"
        r"fra\s+kr\.?\s*([\d.]+)",
        # "Medlemspris fra 11.498 kr."
        r"medlemspris\s+fra[\s\S]{0,40}?([\d.]+)\s*kr",
    ]
    for pat in patterns:
        m = re.search(pat, md, re.IGNORECASE)
        if m:
            price = parse_price(m.group(1))
            if price and price > 5000:
                return price

    # Fallback: smallest price>5000 found anywhere with "kr" suffix
    prices = [parse_price(p) for p in re.findall(r"([\d.]+)\s*kr\b", md, re.IGNORECASE)]
    prices = [p for p in prices if p and p > 5000]
    return min(prices) if prices else None


def _extract_departures(md: str) -> list[dict]:
    """Extract departures from a single product card's markdown table.

    Format (from main product page after JS rendering):
        | 14. mar. 2027 |      | kr. 24.990  |
        | 16. okt. 2026 | kr. 28.495 | Garanteret afgang |
        | 03. nov. 2026 | kr. 43.885 | 2 pladser tilbage |
    """
    departures = []
    seen = set()

    # Pattern: "DD. mmm. YYYY ... kr. NN.NNN ... [optional status]"
    pattern = re.compile(
        r"(\d{1,2}\.\s+[a-zæøå]+\.?\s+\d{4})"     # "14. mar. 2027"
        r"[\s\S]{0,120}?"                          # gap (table cells/whitespace)
        r"kr\.?\s+([\d.]+)"                        # "kr. 24.990" — kr BEFORE number
        r"(?:[\s\S]{0,80}?"                        # optional status block
        r"(Udsolgt|Garanteret\s+afgang|Garanteret|Få\s+pladser|"
        r"\d+\s+pladser?\s+tilbage|"
        r"\d+\s+plads\s+tilbage|Afventer\s+pris))?",
        re.IGNORECASE,
    )

    for m in pattern.finditer(md):
        date_str = m.group(1)
        price_str = m.group(2)
        status_raw = m.group(3) or "Åben"

        start = parse_danish_date(date_str)
        if not start:
            continue
        price = parse_price(price_str)
        if not price or price < 5000:  # filter small numbers
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
            "availability_status": _normalize_stjernegaard_status(status_raw),
            "flight_origin": "København",
            "rejseleder_name": None,
        })

    departures.sort(key=lambda d: d["start_date"])
    return departures


def _normalize_stjernegaard_status(raw: str) -> str:
    """Map Stjernegaard-specific status text to taxonomy enum."""
    s = raw.strip().lower()
    if "udsolgt" in s:
        return "Udsolgt"
    if "garanteret" in s:
        return "Garanteret"
    if "afventer" in s:
        return "Afventer pris"
    # "X pladser tilbage" / "1 plads tilbage" — small numbers signal Få pladser
    m = re.match(r"(\d+)\s+plads", s)
    if m:
        n = int(m.group(1))
        return "Få pladser" if n <= 3 else "Åben"
    if "få" in s:
        return "Få pladser"
    return "Åben"
