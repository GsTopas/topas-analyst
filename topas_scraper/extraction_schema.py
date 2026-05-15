"""
Universal extraction schema for tour-departure data.

This schema is sent to Firecrawl (which uses an LLM internally to populate it
from the rendered page) and to Claude vision (as a JSON-output spec). The same
shape is used for every operator — Topas, Stjernegaard, Albatros, all of them.

This is the architectural shift in v0.8: instead of 9 operator-specific regex
parsers, we have ONE schema. The LLM handles whatever HTML structure each site
uses. When Albatros redesigns their site tomorrow, the regex would break — the
schema doesn't, because LLM reads the visible content the way an analyst does.

Field descriptions are deliberately verbose. They are the prompt to the LLM —
clear descriptions = better extraction quality. Worth more than terse code.
"""

from __future__ import annotations


# The universal tour-extraction schema. Used by both Firecrawl JSON format and
# Claude vision. Every operator's pages get extracted into this same shape.
TOUR_EXTRACTION_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "tour_name": {
            "type": ["string", "null"],
            "description": (
                "The official tour title as displayed on the page (e.g. "
                "'Vandreferie i Annapurna-regionen', 'Albatros Nepal'). "
                "Use the most prominent heading. If multiple tours appear, use "
                "the one that's the focus of the URL. Else null."
            ),
        },
        "duration_days": {
            "type": ["integer", "null"],
            "description": (
                "Total tour duration in DAYS — ONLY use this field if the page "
                "expresses duration as days. Look for '14 dage', '17 days', "
                "'14-dages rejse', '14-day tour'. Return JUST the integer (e.g. 14). "
                "If multiple durations are mentioned (e.g. 'Vælg 12 eller 14 dage'), "
                "return the PRIMARY/headline one. "
                "CRITICAL: if the page instead says 'N nætter' / 'N nights' / "
                "'varighed N nætter', DO NOT populate this field — populate "
                "duration_nights instead and leave duration_days null. "
                "If neither is visible, return null."
            ),
        },
        "duration_nights": {
            "type": ["integer", "null"],
            "description": (
                "Total tour duration in NIGHTS — ONLY use this field if the page "
                "expresses duration as nights (sleeping nights), e.g. 'varighed 7 "
                "nætter', '7 nætter', '7 nights'. Return JUST the integer (e.g. 7). "
                "Do NOT convert to days; the consuming code handles that. "
                "If the page uses 'dage'/'days' instead, leave this null and populate "
                "duration_days. If no duration is visible at all, return null."
            ),
        },
        "from_price_dkk": {
            "type": ["integer", "null"],
            "description": (
                "The lowest 'fra X kr.' (Danish) or 'from X kr.' price visible on "
                "the page, in DKK. Return JUST the integer with no separators "
                "(e.g. 'fra 24.990 kr.' → 24990, 'kr. 23.998' → 23998). "
                "Do NOT include any per-departure prices here — only the headline "
                "from-price. If only per-departure prices exist, return the lowest. "
                "If no price visible at all, return null."
            ),
        },
        "departures": {
            "type": "array",
            "description": (
                "Every departure visible on the page that has BOTH a date AND a "
                "price. Each row in a departure/afgang/priser-og-datoer table = "
                "one entry here. Skip rows that show 'Afventer pris' / 'Pris kommer "
                "snart' / 'TBD' (no price yet). Skip rows for sub-products that are "
                "not the main tour on this page (e.g. 'Forlæng rejsen med 3 dage i "
                "Bali' is an add-on, not a departure). If the page shows no "
                "departure table at all, return an empty array []."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "start_date": {
                        "type": "string",
                        "description": (
                            "Departure date in ISO format YYYY-MM-DD. "
                            "ALL Danish travel sites use European DD.MM.YYYY or "
                            "DD/MM/YYYY format — NEVER American MM/DD/YYYY. "
                            "Conversion examples (preserve the exact day; do NOT "
                            "subtract or add a day, do NOT adjust for timezone): "
                            "  '14. mar. 2027' → '2027-03-14' "
                            "  '29/10/2026'    → '2026-10-29' "
                            "  '04.03.2027'    → '2027-03-04' "
                            "  '06.04.2027'    → '2027-04-06' (NOT 2027-04-05) "
                            "  '17.10.2026'    → '2026-10-17' (NOT 2026-10-16). "
                            "If the source shows '06.04.2027 København' the departure "
                            "is on April 6, 2027 — exactly that calendar day. Output "
                            "must be the same day as the visible source. "
                            "CRITICAL: many sites show afgange as date RANGES (e.g. "
                            "'27. jun → 4. jul 2026' or '14. sep 2026 → 21. sep 2026' "
                            "or 'Afrejseperiode 27. jun - 4. jul'). This is ONE departure, "
                            "not two. Use the FIRST date (start) here and the second date "
                            "(end) in end_date below. NEVER emit two separate entries for "
                            "the same range."
                        ),
                    },
                    "end_date": {
                        "type": ["string", "null"],
                        "description": (
                            "Departure end-date in ISO format YYYY-MM-DD, if the page "
                            "shows the range explicitly (e.g. 'Afrejseperiode 27. jun → "
                            "4. jul 2026' → end_date='2026-07-04'). For sites that only "
                            "show start-date (most operators), return null."
                        ),
                    },
                    "price_dkk": {
                        "type": "integer",
                        "description": (
                            "Price for THIS departure in DKK, basis "
                            "dobbeltværelse / per-person-i-delt-værelse. Return "
                            "just the integer (e.g. '24.990 kr.' → 24990). If the "
                            "row shows multiple prices (single/double), use double."
                        ),
                    },
                    "availability_status": {
                        "type": "string",
                        "enum": [
                            "Garanteret",
                            "Få pladser",
                            "Udsolgt",
                            "Afventer pris",
                            "Åben",
                        ],
                        "description": (
                            "Departure status, mapped to one of these EXACT values. "
                            "Read carefully — different operators use different conventions: "
                            "'Garanteret' = afgang er bekræftet/garanteret/'Garanteret afgang'. "
                            "'Få pladser' = ONLY when explicitly labelled 'Få pladser' OR "
                            "a small number 'X pladser' / 'X pladser tilbage' where X is 1-3 "
                            "(no plus sign). "
                            "'Udsolgt' = sold out / fully booked / 'UDSOLGT' — ONLY when the "
                            "page explicitly says the departure is sold out. NEVER guess "
                            "'Udsolgt' from missing status text or visual cues you can't read. "
                            "'Afventer pris' = date set but price not yet published / "
                            "'Pris kommer snart' / 'Afventer pris'. "
                            "'Åben' = default — booking is open with no scarcity signal. "
                            "Map ALL of these to 'Åben': 'Ledig' (Viktors Farmor), 'Plads' "
                            "(without a small number qualifier), 'Bestil', 'Book nu', or no "
                            "status word at all next to the date/price. "
                            "CRITICAL: '+N pladser' (with a plus sign, e.g. '+8 pladser', "
                            "'+10 pladser') means 'AT LEAST N spots available' — this is "
                            "Smilrejser's way of indicating Åben / plenty of availability. "
                            "Map '+N pladser' to 'Åben', NEVER to 'Få pladser'. "
                            "DEFAULT RULE: If unsure, choose 'Åben'. NEVER default to "
                            "'Udsolgt' — that requires explicit sold-out text on the page."
                        ),
                    },
                    "rejseleder_name": {
                        "type": ["string", "null"],
                        "description": (
                            "Name of the named tour leader assigned to THIS specific "
                            "departure, if shown (e.g. 'Mette Hansen', 'Lars Peter Sørensen'). "
                            "Many operators (Viktors Farmor, Stjernegaard) name their "
                            "rejseledere per departure. Else null."
                        ),
                    },
                    "flight_origin": {
                        "type": ["string", "null"],
                        "description": (
                            "Departure airport, normalized: 'København' (for CPH/Copenhagen/"
                            "København (CPH)') or 'Aalborg' (for AAL/Aalborg) or other city "
                            "name. If not shown, return null."
                        ),
                    },
                },
                "required": ["start_date", "price_dkk", "availability_status"],
            },
        },
    },
    "required": ["departures"],
}
