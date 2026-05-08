"""Operator parser dispatch.

v0.8: ALL parser_keys now resolve to the same generic_ai parser. Operator-
specific regex parsers are deprecated — the LLM-driven extraction handles
every site through one schema (extraction_schema.py).

The parser_key field on TourTarget is preserved as a forward-compatibility
hook in case future operators need custom pre/post-processing. For v0.8,
every key maps to generic_ai.parse.

The legacy operator-specific parser files (topas.py, smilrejser.py, etc.)
remain in the repository for reference but are no longer imported here.
"""

from . import generic_ai

# Universal parser for every operator. Same code for Topas, Stjernegaard,
# Albatros, all of them. Operator-specific knowledge moved to the LLM
# extraction schema (extraction_schema.py).
_UNIVERSAL = generic_ai.parse

PARSERS = {
    "topas": _UNIVERSAL,
    "smilrejser": _UNIVERSAL,
    "jysk": _UNIVERSAL,
    "viktorsfarmor": _UNIVERSAL,
    "ruby": _UNIVERSAL,
    "nillesgislev": _UNIVERSAL,
    "stjernegaard": _UNIVERSAL,
    "albatros": _UNIVERSAL,
    "besttravel": _UNIVERSAL,
    "fyrholt": _UNIVERSAL,
    "gjoa": _UNIVERSAL,
    "kipling": _UNIVERSAL,
    "vagabond": _UNIVERSAL,
    "generic_ai": _UNIVERSAL,
}
