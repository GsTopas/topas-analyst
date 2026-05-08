"""
Base parser contract and shared helpers.

Each operator parser is a single function:

    def parse(scrape: ScrapeResult, target: TourTarget) -> ParseOutput:
        ...

ParseOutput is a tuple of (tour_dict, departures_list). The tour_dict matches
the schema in db.py:upsert_tour. The departures_list is a list of dicts with
the schema in db.py:replace_departures.

Helpers below handle the per-operator quirks that are *not* operator-specific:
parsing Danish month names, normalizing prices, normalizing status strings.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Optional


# === Danish month-name parsing ===
DANISH_MONTHS = {
    "jan": 1, "januar": 1,
    "feb": 2, "februar": 2,
    "mar": 3, "marts": 3,
    "apr": 4, "april": 4,
    "maj": 5,
    "jun": 6, "juni": 6,
    "jul": 7, "juli": 7,
    "aug": 8, "august": 8,
    "sep": 9, "september": 9,
    "okt": 10, "oktober": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def parse_danish_date(s: str) -> Optional[date]:
    """Parse strings like '17. juli 2026', '17. jul 2026', '17. juli 26', '17/07/2026'.

    Returns None if no date pattern matched. Tolerates extra whitespace and
    different separators.
    """
    if not s:
        return None
    s = s.strip().lower()

    # Format: 17. juli 2026 / 17 juli 2026 / 17. jul. 2026
    m = re.search(r"(\d{1,2})\.?\s+([a-zæøå]+)\.?\s+(\d{2,4})", s)
    if m:
        day, month_name, year = m.group(1), m.group(2), m.group(3)
        month = DANISH_MONTHS.get(month_name)
        if month:
            try:
                y = int(year)
                if y < 100:
                    y += 2000
                return date(y, month, int(day))
            except ValueError:
                return None

    # Format: 17/07/2026 or 17-07-2026
    m = re.search(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{2,4})", s)
    if m:
        try:
            day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if year < 100:
                year += 2000
            return date(year, month, day)
        except ValueError:
            return None

    return None


def parse_price(s: str) -> Optional[int]:
    """Extract an integer DKK price from strings like '12.995 DKK', '13.470 kr.', 'fra 13.970'."""
    if not s:
        return None
    # Find the longest run of digits (with thousand-separator dots/spaces removed)
    cleaned = s.replace(".", "").replace(" ", "").replace(",", "")
    m = re.search(r"(\d{4,6})", cleaned)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


# === Status normalization ===
# Maps operator-specific text to the canonical taxonomy enum.
STATUS_MAP = {
    # Direct matches
    "garanteret afgang": "Garanteret",
    "garanteret": "Garanteret",
    "få pladser": "Få pladser",
    "udsolgt": "Udsolgt",
    "åben for booking": "Åben",
    "ledig": "Åben",
    "åben": "Åben",
    "afventer pris": "Afventer pris",
    "pris kommer snart": "Afventer pris",
    "på forespørgsel": "På forespørgsel",
    "kontakt os": "På forespørgsel",
    "ingen planlagte afgange": "På forespørgsel",
    # Smilrejser booking-flow text
    "+8 pladser": "Åben",
    "+5 pladser": "Få pladser",
    "+3 pladser": "Få pladser",
}


def normalize_status(raw: str) -> str:
    """Map operator-specific status text to canonical enum."""
    if not raw:
        return "Ukendt"
    s = raw.strip().lower()
    # Direct lookup first
    if s in STATUS_MAP:
        return STATUS_MAP[s]
    # Substring matching for longer strings
    for key, canonical in STATUS_MAP.items():
        if key in s:
            return canonical
    return raw.strip()


@dataclass
class ParsedTour:
    """Output container — convertible to dict via to_dict().

    Identity/geography/relationship fields (country, region, competes_with,
    tour_code) are sourced from the TourTarget config — NOT from page parsing.
    Use make_parsed_tour() to construct so these fields are always set
    correctly. Parsers should only supply page-extracted data.
    """

    operator: str
    tour_slug: str
    tour_name: str
    url: str
    country: str                              # Required — from TourTarget
    competes_with: str                        # Required — Topas tour-code this competes against
    tour_code: Optional[str] = None
    region: Optional[str] = None
    tour_format: str = "Fællesrejse"
    duration_days: Optional[int] = None
    from_price_dkk: Optional[int] = None
    fællesrejse_eligible: bool = True
    eligibility_notes: str = ""

    def to_dict(self) -> dict:
        return {
            "operator": self.operator,
            "tour_slug": self.tour_slug,
            "tour_name": self.tour_name,
            "url": self.url,
            "tour_code": self.tour_code,
            "country": self.country,
            "region": self.region,
            "competes_with": self.competes_with,
            "tour_format": self.tour_format,
            "duration_days": self.duration_days,
            "from_price_dkk": self.from_price_dkk,
            "fællesrejse_eligible": self.fællesrejse_eligible,
            "eligibility_notes": self.eligibility_notes,
        }


def make_parsed_tour(target, *, operator: str, **page_data) -> ParsedTour:
    """Build a ParsedTour with target-driven metadata pre-filled.

    Parsers MUST use this constructor instead of calling ParsedTour() directly.
    It guarantees that country, region, competes_with, and tour_code come from
    the authoritative TourTarget config — eliminating the entire class of bugs
    where a parser hardcodes/defaults a wrong country.

    Parsers supply only:
      - operator (the operator name as it should appear in the UI)
      - page_data: duration_days, from_price_dkk, eligibility_notes,
                   fællesrejse_eligible, tour_format
      - tour_code (optional override) — for competitors with their own internal
        codes (e.g. Ruby's "VAG-053"). Defaults to target.tour_code when omitted.

    Example:
        tour = make_parsed_tour(
            target,
            operator="Smilrejser",
            duration_days=_extract_duration(md),
            from_price_dkk=_extract_from_price(md),
            eligibility_notes="Aller Leisure brand — Tier 1 anchor for PTMD.",
        )
    """
    # Allow parser to override tour_code (Ruby's VAG-053 etc.); else use target's.
    tour_code = page_data.pop("tour_code", None) or target.tour_code

    return ParsedTour(
        operator=operator,
        tour_slug=slugify(target.tour_name),
        tour_name=target.tour_name,
        url=target.url,
        tour_code=tour_code,
        country=target.country,
        region=target.region,
        competes_with=target.competes_with,
        **page_data,
    )


def slugify(text: str) -> str:
    """Make a stable, lowercase, URL-safe slug for tour identification."""
    s = text.lower().strip()
    s = re.sub(r"[æøå]", lambda m: {"æ": "ae", "ø": "oe", "å": "aa"}[m.group(0)], s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")
