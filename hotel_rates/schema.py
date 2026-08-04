"""
Extraction-schema for hotel-rate-sider (én nat, 2 voksne).

Samme princip som topas_scraper.extraction_schema: schemaet ER prompten
til LLM'en (Firecrawl JSON-format eller Claude vision). Verbose felt-
beskrivelser = bedre extraction. Ét universelt schema for alle engines.
"""
from __future__ import annotations


RATE_EXTRACTION_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "currency": {
            "type": ["string", "null"],
            "description": (
                "The currency code the prices on the page are displayed in "
                "(e.g. 'DKK', 'EUR', 'USD'). Look at the price labels — "
                "'DKK 2,690' means currency='DKK'. If no prices visible, null."
            ),
        },
        "arrival_date_shown": {
            "type": ["string", "null"],
            "description": (
                "The check-in/arrival date the page VISIBLY DISPLAYS for this "
                "search, converted to ISO YYYY-MM-DD (e.g. 'Arrival: September "
                "15, 2026' → '2026-09-15', 'Jul 22' with the year from context "
                "→ '2026-07-22'). CRITICAL: read it from the page's rendered "
                "text/labels ONLY — NEVER from the URL or link parameters (the "
                "URL may deliberately contain a different date). If no arrival "
                "date is visibly displayed on the page, return null."
            ),
        },
        "sold_out": {
            "type": "boolean",
            "description": (
                "True ONLY if the page explicitly states there is NO "
                "availability for the requested dates (e.g. 'No rooms "
                "available', 'Sold out', 'Ingen ledige værelser', 'no "
                "availability for the selected dates'). False if any room "
                "with a price is shown, or if the page simply failed to load "
                "rates. NEVER guess sold_out from missing content."
            ),
        },
        "rooms": {
            "type": "array",
            "description": (
                "Every room offer visible on the page for the requested stay. "
                "One entry per room type / rate shown with a price. If the "
                "same room type appears with multiple rates (e.g. flexible vs "
                "non-refundable), include EACH as its own entry. If no rooms "
                "with prices are shown, return an empty array []."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "room_name": {
                        "type": "string",
                        "description": (
                            "The room type name as displayed (e.g. 'Standard "
                            "Double Room', 'Superior Room with sea view', "
                            "'Single Room'). Include rate-plan qualifier if "
                            "shown (e.g. 'Standard Double — Non-refundable')."
                        ),
                    },
                    "price": {
                        "type": "integer",
                        "description": (
                            "TOTAL price for the stay as an integer with NO "
                            "thousand separators and NO decimals ('DKK 2,690' "
                            "→ 2690, '2.690 kr.' → 2690, '2,595.00 DKK' → "
                            "2595 — the .00 is decimals, DROP them, never "
                            "read 2,595.00 as 259500). Use the price for the "
                            "whole requested stay (for a 1-night search this "
                            "equals the nightly rate). If both per-night and "
                            "total are shown, use the total."
                        ),
                    },
                    "availability_status": {
                        "type": "string",
                        "enum": ["Åben", "Få pladser", "Udsolgt"],
                        "description": (
                            "'Udsolgt' ONLY if this specific room is "
                            "explicitly marked sold out / unavailable. "
                            "'Få pladser' ONLY if the page shows scarcity "
                            "like 'Only 2 rooms left' / 'Kun 1 tilbage' with "
                            "a number 1-3. Otherwise 'Åben' (default — a "
                            "bookable room with a price and no scarcity "
                            "signal is 'Åben')."
                        ),
                    },
                },
                "required": ["room_name", "price", "availability_status"],
            },
        },
    },
    "required": ["rooms", "sold_out"],
}
