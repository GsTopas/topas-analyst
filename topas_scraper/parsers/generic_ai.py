"""
Generic AI-driven parser — works for ALL operators.

This is the architectural shift in v0.8: instead of writing operator-specific
regex parsers (one per Topas, Smilrejser, Stjernegaard, Albatros, Viktors
Farmor, Ruby, Jysk, Nilles & Gislev, Best Travel = 9 parsers totalling ~1500
lines of brittle regex), we have ONE parser that consumes Firecrawl's
LLM-extracted JSON and converts it to our internal format.

Per methodology section 0.2: tool must match analyst's reach. The LLM reads
the page the way an analyst would. When a site is redesigned, the regex
parsers would break — this doesn't, because the schema description is
operator-agnostic.

Cost: ~$0.005 per scrape (Firecrawl's LLM markup over plain markdown). For
19 URLs × weekly = ~$5/year. Negligible vs. parser-maintenance time saved.

When this parser returns 0 departures, runner.py invokes vision_extractor as
Tier 3 fallback (still useful for pages where Firecrawl can't even render the
content — e.g. Albatros's React-heavy pages with anti-bot measures).
"""

from __future__ import annotations

import re
from typing import Any, Optional

from .base import make_parsed_tour, ParsedTour


# Valid availability statuses (mirrors extraction_schema.py enum). If the LLM
# returns something unexpected, we map to the closest match or default to "Åben".
_VALID_STATUSES = {"Garanteret", "Få pladser", "Udsolgt", "Afventer pris", "Åben"}


def parse(scrape, target) -> tuple[dict, list[dict]]:
    """Generic parser — works for any operator using Firecrawl JSON extraction.

    Reads scrape.extracted (populated by Firecrawl's LLM extraction) and
    converts to the (tour_dict, departures_list) tuple expected by runner.py.

    If scrape.extracted is None or empty, returns the tour with 0 departures.
    runner.py will then invoke Tier 3 vision fallback.
    """
    extracted = scrape.extracted or {}

    # Build the tour dict from extracted page data.
    #
    # Duration: schema now has TWO fields — duration_days for sites that say
    # "N dage", duration_nights for sites that say "N nætter" (Ruby Rejser
    # is the canonical example). Convert nights→days here deterministically
    # rather than trusting the LLM to do arithmetic. A 7-nætter tour = 8 dage
    # (depart day 1, fly home day 8, with 7 nights in between).
    duration_days = _safe_int(extracted.get("duration_days"))
    if duration_days is None:
        nights = _safe_int(extracted.get("duration_nights"))
        if nights is not None:
            duration_days = nights + 1

    # Defensive: even if the LLM ignored the schema instruction and put nights
    # into duration_days, scan the raw markdown for explicit nights patterns.
    # If we find "varighed N nætter" or "N nætter" with N matching what the LLM
    # returned, we override with nights+1. This is how Ruby's site is written
    # and the LLM has been observed to return the raw nights count there.
    md = getattr(scrape, "markdown", None) or ""
    if md:
        nights_from_md = _detect_nights_in_markdown(md)
        if nights_from_md is not None:
            # If the LLM's duration_days IS the nights count, fix it up.
            # If duration_days is None, populate from nights.
            # If duration_days is already nights+1 (or something else sensible),
            # leave it alone.
            if duration_days is None or duration_days == nights_from_md:
                duration_days = nights_from_md + 1

    from_price_dkk = _safe_int(extracted.get("from_price_dkk"))

    raw_departures = extracted.get("departures") or []
    departures = _normalize_departures(raw_departures)

    # Eligibility notes — descriptive of what we got, useful in the UI
    if departures:
        notes = (
            f"Extracted via Firecrawl JSON · {len(departures)} departures · "
            f"duration {duration_days or '?'} dage · from-pris "
            f"{from_price_dkk or '?'} kr."
        )
    elif from_price_dkk:
        notes = (
            f"Headline only — {duration_days or '?'} dage fra {from_price_dkk} kr. "
            f"Per-departure data not extracted (LLM returned 0 departures)."
        )
    else:
        notes = (
            "No data extracted via LLM. Page may be empty, JS-blocked, or "
            "behind anti-bot. Tier 3 vision fallback will be tried."
        )

    tour = make_parsed_tour(
        target,
        operator=target.operator,
        duration_days=duration_days,
        from_price_dkk=from_price_dkk,
        eligibility_notes=notes,
    )

    return tour.to_dict(), departures


def _normalize_departures(raw: list) -> list[dict]:
    """Convert LLM-extracted departures into our internal format.

    Validates each item has required fields. Drops invalid rows (rather than
    crashing the whole parse). Deduplicates on start_date. Sorts chronologically.
    """
    if not isinstance(raw, list):
        return []

    seen_dates: set[str] = set()
    out: list[dict] = []

    for item in raw:
        if not isinstance(item, dict):
            continue

        start_date = _safe_date_str(item.get("start_date"))
        price_dkk = _safe_int(item.get("price_dkk"))
        if not start_date or price_dkk is None:
            # Required fields missing — skip this row. The LLM sometimes returns
            # placeholder rows for "Afventer pris" that we want to filter out.
            continue

        if start_date in seen_dates:
            continue
        seen_dates.add(start_date)

        status = _normalize_status(item.get("availability_status"))
        flight_origin = _safe_str(item.get("flight_origin")) or "København"
        rejseleder = _safe_str(item.get("rejseleder_name"))

        out.append({
            "departure_code": None,
            "start_date": start_date,
            "end_date": None,
            "price_dkk": price_dkk,
            "availability_status": status,
            "flight_origin": flight_origin,
            "rejseleder_name": rejseleder,
        })

    out.sort(key=lambda d: d["start_date"])
    return out


def _detect_nights_in_markdown(md: str) -> Optional[int]:
    """Scan markdown for 'varighed N nætter' or 'N nætter' / 'N nights'.

    Returns the integer N if found, else None. Prefer 'varighed' anchor —
    that's the headline duration. Bare 'N nætter' would catch room-stay
    descriptions ('3 nætter på hotel') so we constrain to short numbers
    (2-21 nights, the realistic range for guided tours).
    """
    # Anchored: "varighed 7 nætter" / "varighed 7 nights"
    m = re.search(r"varighed\s+(\d{1,2})\s+(?:nætter|nights)", md, re.IGNORECASE)
    if m:
        n = int(m.group(1))
        if 2 <= n <= 21:
            return n
    # Less specific: "7 nætter" appearing in a duration-like context
    # (only trust if it appears near the top of the page or in a heading)
    m = re.search(r"(?:^|\n)\s*#{1,6}\s+[^\n]*?(\d{1,2})\s+nætter", md, re.IGNORECASE)
    if m:
        n = int(m.group(1))
        if 2 <= n <= 21:
            return n
    return None


def _safe_int(value: Any) -> Optional[int]:
    """Coerce to int. Returns None if value is None/empty/unparseable."""
    if value is None:
        return None
    if isinstance(value, bool):  # bool is subtype of int — exclude
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float):
        return int(value) if value > 0 else None
    if isinstance(value, str):
        # Strip currency symbols, separators
        cleaned = "".join(c for c in value if c.isdigit())
        if cleaned:
            try:
                n = int(cleaned)
                return n if n > 0 else None
            except ValueError:
                return None
    return None


def _safe_str(value: Any) -> Optional[str]:
    """Return non-empty stripped string or None."""
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    s = value.strip()
    return s if s else None


def _safe_date_str(value: Any) -> Optional[str]:
    """Validate ISO date format YYYY-MM-DD. Pass through if valid, else None.

    The schema instructs the LLM to return ISO format. If it deviates (some
    edge cases in the prompt), drop the row — better to lose a departure than
    poison the database with garbage dates.
    """
    if not isinstance(value, str):
        return None
    s = value.strip()
    # Lightweight ISO check: YYYY-MM-DD with year 2024-2099, month 01-12, day 01-31
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        return None
    try:
        year = int(s[:4])
        month = int(s[5:7])
        day = int(s[8:10])
    except ValueError:
        return None
    if not (2024 <= year <= 2099):
        return None
    if not (1 <= month <= 12):
        return None
    if not (1 <= day <= 31):
        return None
    return s


def _normalize_status(value: Any) -> str:
    """Map LLM-returned status to one of the 5 valid enum values."""
    if isinstance(value, str) and value in _VALID_STATUSES:
        return value
    if not isinstance(value, str):
        return "Åben"
    s = value.strip().lower()
    # CRITICAL: '+N pladser' (with plus sign) means 'at least N spots available'
    # — this is Smilrejser's open-availability convention, NOT scarcity.
    # Must match BEFORE the 'få plad' lenient match below, since '+8 pladser'
    # contains 'plad' but is semantically Åben.
    import re
    if re.search(r"\+\s*\d+\s*plad", s):
        return "Åben"
    # Lenient matching for cases where the LLM didn't strictly follow the enum
    if "garant" in s or "confirmed" in s:
        return "Garanteret"
    if "udsolg" in s or "sold out" in s or "fully booked" in s:
        return "Udsolgt"
    if "få plad" in s or "limited" in s or "few spots" in s or "low avail" in s:
        return "Få pladser"
    if "afvent" in s or "pending" in s or "tbd" in s:
        return "Afventer pris"
    return "Åben"
