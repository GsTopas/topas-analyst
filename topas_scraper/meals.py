"""
Måltids-extraction fra scraped tour-markdown.

Strategi: pattern-first, AI som fallback.

1. Detect operatør fra URL eller markdown
2. Kør operatør-specifik extractor (præcis regex på det format vi har set)
3. Hvis intet match: AI-fallback (Sonnet 4.6) med forbedret prompt
4. Sidste fallback: tom

Operatør-specifikke extractors er bygget på faktisk site-research (se
outputs/meals_research/). Eksempler:

  - Smilrejser:    "8 All Inclusive · 4 Frokost"
  - Stjernegaard:  "12 x morgenmad · 9 x frokost · 6 x middag"
  - Jysk:          M/F/A per dag (samme som Topas)
  - Topas:         M/F/A per dag

Output:
    {"mealsCount": int|None, "mealsSummary": str, "extractionMethod": str}

extractionMethod-feltet giver telemetri: hvilken path virkede — nyttigt til
debugging og iteration på de hårde cases.
"""

from __future__ import annotations

import json
import os
import re
from typing import Optional, Tuple
from urllib.parse import urlparse

try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None  # type: ignore


# ---------------------------------------------------------------------------
# Operator detection
# ---------------------------------------------------------------------------

DOMAIN_TO_OPERATOR = {
    "smilrejser.dk": "smilrejser",
    "jysk-rejsebureau.dk": "jysk",
    "viktorsfarmor.dk": "viktorsfarmor",
    "ruby-rejser.dk": "ruby",
    "stjernegaard-rejser.dk": "stjernegaard",
    "albatros.dk": "albatros",
    "kiplingtravel.dk": "kipling",
    "fyrholtrejser.dk": "fyrholt",   # NB: uden bindestreg
    "vagabondtours.dk": "vagabond",
    "gjoa.dk": "gjoa",
    "topas.dk": "topas",
}


def _detect_operator(url: str | None, markdown: str = "") -> str | None:
    """Return operator key from URL, falling back to markdown sniffing."""
    if url:
        try:
            host = urlparse(url).netloc.lower()
            host = host.removeprefix("www.")
            for domain, op in DOMAIN_TO_OPERATOR.items():
                if domain in host:
                    return op
        except Exception:  # noqa: BLE001
            pass

    md_l = (markdown or "").lower()[:5000]
    for domain, op in DOMAIN_TO_OPERATOR.items():
        if domain in md_l:
            return op
    return None


# ---------------------------------------------------------------------------
# Operator-specific regex extractors
# ---------------------------------------------------------------------------

def _extract_smilrejser(md: str) -> Optional[Tuple[int, str]]:
    """Smilrejser: 'Det får du med: 8 All Inclusive · 4 Frokost' osv."""
    md_l = md.lower()

    ai_match = re.search(r"\b(\d+)\s*all[\s\-]?inclusive\b", md_l)
    frokost_match = re.search(r"\b(\d+)\s*frokost\b", md_l)
    middag_match = re.search(r"\b(\d+)\s*middag", md_l)
    morgen_match = re.search(r"\b(\d+)\s*morgenmad", md_l)

    ai_count = int(ai_match.group(1)) if ai_match else 0
    frokost_count = int(frokost_match.group(1)) if frokost_match else 0
    middag_count = int(middag_match.group(1)) if middag_match else 0
    morgen_count = int(morgen_match.group(1)) if morgen_match else 0

    # All Inclusive trumps everything — hver AI-dag tæller som 3 måltider
    if ai_count > 0:
        parts = [f"{ai_count} All Inclusive"]
        if frokost_count > 0:
            parts.append(f"{frokost_count} frokost")
        total = ai_count * 3
        return (total, " · ".join(parts))

    # Ellers: tæl morgenmad/frokost/middag separat
    counts = []
    total = 0
    if morgen_count:
        counts.append(f"{morgen_count} morgenmad")
        total += morgen_count
    if frokost_count:
        counts.append(f"{frokost_count} frokost")
        total += frokost_count
    if middag_count:
        counts.append(f"{middag_count} middag")
        total += middag_count

    if counts:
        return (total, " · ".join(counts))
    return None


def _extract_stjernegaard(md: str) -> Optional[Tuple[int, str]]:
    """Stjernegaard lister måltider per dag på /dagsprogram/-siden som
    bullet-points: '- Morgenmad', '- Frokost', '- Middag'. Hver bullet =
    ét inkluderet måltid for den pågældende dag.

    Forudsætter at runner.py har concatenat dagsprogram-markdown'en til
    main page-markdown'en (gøres pga. Stjernegaard's split mellem oversigt
    og dagsprogram-undersider).

    Falder tilbage til legacy '12 x morgenmad'-mønsteret hvis bullet-form
    ikke findes — bevarer support for evt. ældre tour-formater.
    """
    # Primær metode: tæl bullet-list på dagsprogram-siden
    bullet_morgen = len(re.findall(r"^[\-\*]\s+Morgenmad\s*$", md, re.MULTILINE))
    bullet_frokost = len(re.findall(r"^[\-\*]\s+Frokost\s*$", md, re.MULTILINE))
    bullet_middag = len(re.findall(r"^[\-\*]\s+Middag\s*$", md, re.MULTILINE))

    if bullet_morgen + bullet_frokost + bullet_middag > 0:
        parts = []
        total = 0
        if bullet_morgen:
            parts.append(f"{bullet_morgen} morgenmad")
            total += bullet_morgen
        if bullet_frokost:
            parts.append(f"{bullet_frokost} frokost")
            total += bullet_frokost
        if bullet_middag:
            parts.append(f"{bullet_middag} middag")
            total += bullet_middag
        return (total, " · ".join(parts))

    # Legacy fallback: ældre format '12 x morgenmad'
    md_l = md.lower()
    morgen = re.search(r"(\d+)\s*x\s*morgenmad", md_l)
    frokost = re.search(r"(\d+)\s*x\s*frokost", md_l)
    middag = re.search(r"(\d+)\s*x\s*middag", md_l)

    if not (morgen or frokost or middag):
        return None

    parts = []
    total = 0
    if morgen:
        n = int(morgen.group(1))
        parts.append(f"{n} morgenmad")
        total += n
    if frokost:
        n = int(frokost.group(1))
        parts.append(f"{n} frokost")
        total += n
    if middag:
        n = int(middag.group(1))
        parts.append(f"{n} middag")
        total += n

    return (total, " · ".join(parts))


def _extract_mfa_per_day(md: str) -> Optional[Tuple[int, str]]:
    """Topas/Jysk-stil: pr. dag står 'M/F/A', 'M/-/-', '(M,F,A)', '(A)' osv.
    Tæl alle M/F/A i sådanne markører — uden double-counting."""
    morgen_count = 0
    frokost_count = 0
    aften_count = 0
    days_seen = 0
    consumed: list[tuple[int, int]] = []

    def _overlaps(start: int, end: int) -> bool:
        return any(s <= start < e or s < end <= e for s, e in consumed)

    # Pattern 1: tre-slot markør "M/F/A", "M,F,A", "M/-/-", "-/F/-" osv.
    # Bruger parenteser eller whitespace omkring som anchor, så vi ikke
    # matcher mid-prosa.
    pattern1 = re.compile(
        r"(?:^|[\s\(\[\*])"
        r"([M\-])\s*[/,]\s*([F\-])\s*[/,]\s*([A\-])"
        r"(?=[\s\)\]\.\,\*]|$)",
        re.MULTILINE,
    )
    for m in pattern1.finditer(md):
        m_, f_, a_ = m.groups()
        if m_ == "M":
            morgen_count += 1
        if f_ == "F":
            frokost_count += 1
        if a_ == "A":
            aften_count += 1
        days_seen += 1
        consumed.append((m.start(), m.end()))

    # Pattern 2: paren-wrappet markør "(M)", "(A)", "(M,F,A)", "(M/F)", osv.
    # Skipper overlaps med pattern1, så "(M,F,A)" ikke tælles to gange.
    # Skipper også "(M = Morgenmad...)" pga. character class.
    pattern2 = re.compile(r"\(([MFA, /\-\s]+)\)")
    for m in pattern2.finditer(md):
        if _overlaps(m.start(), m.end()):
            continue
        content = m.group(1).upper().strip()
        # Skal indeholde mindst ét M/F/A og kun "lovlige" chars
        if not re.fullmatch(r"[MFA,/\-\s]+", content):
            continue
        if not re.search(r"[MFA]", content):
            continue
        if "M" in content:
            morgen_count += 1
        if "F" in content:
            frokost_count += 1
        if "A" in content:
            aften_count += 1
        days_seen += 1
        consumed.append((m.start(), m.end()))

    if days_seen == 0:
        return None

    total = morgen_count + frokost_count + aften_count
    if total == 0:
        return None

    parts = []
    if morgen_count:
        parts.append(f"{morgen_count} morgenmad")
    if frokost_count:
        parts.append(f"{frokost_count} frokost")
    if aften_count:
        parts.append(f"{aften_count} aftensmad")

    return (total, " · ".join(parts))


def _extract_viktorsfarmor(md: str) -> Optional[Tuple[int, str]]:
    """Viktors Farmor bruger eksplicit prosa-summary i pris-inkluderet:
    'Halvpension dag 1, 3 og 6. Helpension dag 2, 5 og 7. Kun morgenmad dag 4 og 8.'

    Vi parser dage pr. pension-type og summer:
      - Halvpension = 2 måltider/dag (M+A typisk)
      - Helpension/Fuldpension = 3 måltider/dag
      - Kun morgenmad = 1 måltid/dag
      - All Inclusive = 99 (special-flag)
    """
    md_l = md.lower()

    def _count_days(pattern: str) -> int:
        """Tæl dage fra '... dag 1, 3 og 6' ELLER '... dag 1-7' ELLER '... alle dage'."""
        m = re.search(pattern, md_l)
        if not m:
            return 0
        rest = m.group(1)
        # 'alle dage' eller 'hele turen' fanges ikke her — håndteres separat
        # Tæl tal-referencer i restens slut
        nums = re.findall(r"\b\d+\b", rest[:200])
        return len(set(nums))

    # All Inclusive eller 'alle måltider'
    if re.search(r"all\s*[\-\s]*inclusive", md_l):
        return (99, "All Inclusive")

    halv_days = _count_days(r"halvpension(?:\s+(?:på|i))?\s+dag\s+([\d,\sog\-\.]+)")
    hel_days = _count_days(r"(?:hel|fuld)pension(?:\s+(?:på|i))?\s+dag\s+([\d,\sog\-\.]+)")
    morgen_only_days = _count_days(r"kun\s+morgenmad(?:\s+(?:på|i))?\s+dag\s+([\d,\sog\-\.]+)")

    if not (halv_days or hel_days or morgen_only_days):
        # Fald-back: er der bare 'halvpension' eller 'fuldpension' i prosa
        # uden specifikke dage? Så markér med ukendt count men kendt type.
        if "fuldpension" in md_l or "fuld pension" in md_l:
            return (-1, "Fuldpension") if False else None  # too vague
        if "halvpension" in md_l or "halv pension" in md_l:
            return None
        return None

    total = halv_days * 2 + hel_days * 3 + morgen_only_days * 1
    parts = []
    if halv_days:
        parts.append(f"{halv_days} dage halvpension")
    if hel_days:
        parts.append(f"{hel_days} dage helpension")
    if morgen_only_days:
        parts.append(f"{morgen_only_days} dage kun morgenmad")
    return (total, " · ".join(parts))


def _extract_kipling(md: str) -> Optional[Tuple[int, str]]:
    """Kipling Travel: prosa per dag, fx
       'Morgenmad er inkluderet.'
       'Morgenmad, frokost og aftensmad er inkluderet.'
    Vi tæller forekomster af inklusions-sætninger."""
    md_l = md.lower()
    morgen_count = 0
    frokost_count = 0
    aften_count = 0

    # Match patterns: "<combo> er inkluderet"
    # Combo = liste af morgenmad/frokost/aftensmad/middag adskilt af komma og 'og'
    pattern = re.compile(
        r"((?:morgenmad|frokost|aftensmad|middag)"
        r"(?:[\s,og]+(?:morgenmad|frokost|aftensmad|middag))*)"
        r"\s+er\s+inkluderet",
        re.IGNORECASE,
    )

    days_seen = 0
    for m in pattern.finditer(md_l):
        combo = m.group(1)
        if "morgenmad" in combo:
            morgen_count += 1
        if "frokost" in combo:
            frokost_count += 1
        if "aftensmad" in combo or "middag" in combo:
            aften_count += 1
        days_seen += 1

    if days_seen == 0:
        return None

    total = morgen_count + frokost_count + aften_count
    if total == 0:
        return None

    parts = []
    if morgen_count:
        parts.append(f"{morgen_count} morgenmad")
    if frokost_count:
        parts.append(f"{frokost_count} frokost")
    if aften_count:
        parts.append(f"{aften_count} aftensmad")
    return (total, " · ".join(parts))


def _extract_ruby(md: str) -> Optional[Tuple[int, str]]:
    """Ruby Rejser: 'De 6 middage er naturligvis inkluderet i prisen'."""
    md_l = md.lower()

    m = re.search(r"de\s+(\d+)\s+middage?\s+(?:er\s+)?(?:naturligvis\s+)?(?:inkluderet|inklusive)", md_l)
    if m:
        n = int(m.group(1))
        return (n, f"{n} middag")
    m = re.search(r"(\d+)\s+(?:fælles\-?)?middage", md_l)
    if m:
        n = int(m.group(1))
        return (n, f"{n} middag")
    return None


# ---------------------------------------------------------------------------
# AI fallback med forbedret prompt
# ---------------------------------------------------------------------------

_AI_PROMPT = """\
Udtræk måltidsinformation fra en dansk rejsebureau-side.

OUTPUT-REGEL (kritisk): Svar KUN med ét gyldigt JSON-objekt, intet andet.
Ingen forklaring, ingen markdown, ingen prosa før eller efter. Format:
{"mealsCount": <heltal>, "mealsSummary": "<dansk tekst max 80 tegn>"}

mealsCount = samlet antal inkluderede måltider:
  - "Morgenmad alle dage" på 8-dages tur = 8
  - "Halvpension" på 8-dages tur = 14 (M+A pr. dag)
  - "Fuldpension" på 8-dages tur = 21 (M+F+A pr. dag)
  - "All Inclusive" hele turen = 99
  - "6 middage" = 6
  - "Inkluderer ikke måltider" = 0
  - Genuint ikke nævnt = -1

FORMAT-EKSEMPLER:
- "8 All Inclusive · 4 Frokost" → 24
- "12 x morgenmad, 9 x frokost, 6 x middag" → 27
- "(M,F,A) (A) (M) (M)" → 6
- "Halvpension dag 1,3,6. Helpension dag 2,5,7. Kun morgenmad dag 4,8" → 17
- "De 6 middage er inkluderet" → 6
- Albatros prosa: tæl manuelt fra dagsprogram (morgenmad + frokost + middag-omtaler)

mealsSummary = kort dansk label, fx "All Inclusive", "Halvpension",
"12 morgenmad · 9 frokost · 6 middag". Tom string hvis ikke nævnt.

Side-indhold:
---
%s
---

Husk: Svar KUN med JSON-objektet, intet andet."""


def _ai_extract(markdown: str, model: str = "claude-sonnet-4-6") -> Optional[Tuple[int, str]]:
    """Anthropic LLM extraction. Returns None hvis ANTHROPIC_API_KEY mangler eller fejler.

    Hvis MEALS_AI_DEBUG=1 i miljøet, printes konkrete fejl til stdout — nyttigt
    til at debugge hvorfor AI-fallback ikke kører."""
    debug = os.environ.get("MEALS_AI_DEBUG") == "1"

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        if debug:
            print("  [AI-debug] ANTHROPIC_API_KEY mangler i miljoeet")
        return None
    if Anthropic is None:
        if debug:
            print("  [AI-debug] anthropic-pakken ikke installeret (pip install anthropic)")
        return None

    # Tool-based extraction: tvinger struktureret output via Anthropic's tool-use.
    submit_tool = {
        "name": "submit_meals",
        "description": "Indberet måltidsinformation udtrukket fra rejsebureau-siden.",
        "input_schema": {
            "type": "object",
            "properties": {
                "mealsCount": {
                    "type": "integer",
                    "description": "Samlet antal måltider inkluderet i prisen. -1 hvis ikke nævnt. 99 for All Inclusive.",
                },
                "mealsSummary": {
                    "type": "string",
                    "description": "Kort dansk beskrivelse, max 80 tegn (fx 'Halvpension', 'All Inclusive', '12 morgenmad · 9 frokost · 6 middag'). Tom string hvis ikke nævnt.",
                },
            },
            "required": ["mealsCount", "mealsSummary"],
        },
    }

    try:
        client = Anthropic(api_key=api_key)
        snippet = markdown[:40000]
        resp = client.messages.create(
            model=model,
            max_tokens=400,
            temperature=0,
            tools=[submit_tool],
            tool_choice={"type": "tool", "name": "submit_meals"},
            messages=[{"role": "user", "content": _AI_PROMPT % snippet}],
        )

        # Find tool_use block i svaret
        data = None
        for block in resp.content:
            if hasattr(block, "type") and block.type == "tool_use":
                data = block.input
                break

        if debug:
            print(f"  [AI-debug] tool-use input: {data}")

        if not data:
            if debug:
                print("  [AI-debug] Intet tool_use-block i svaret")
            return None
        mc = data.get("mealsCount")
        if isinstance(mc, int) and mc < 0:
            mc = None
        summary = (data.get("mealsSummary") or "").strip()[:80]
        if mc is None and not summary:
            if debug:
                print("  [AI-debug] LLM returnerede tom data")
            return None
        return (mc if isinstance(mc, int) else 0, summary)
    except Exception as exc:
        if debug:
            print(f"  [AI-debug] {type(exc).__name__}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_meals(
    markdown: str,
    *,
    url: str | None = None,
    model: str = "claude-sonnet-4-6",
) -> dict:
    """Extract meal info. Returns:
        {"mealsCount": int|None, "mealsSummary": str, "extractionMethod": str}

    Strategi:
      1. Operator-detect -> koer operator-specific regex
      2. Hvis intet: proev generisk M/F/A-counter (Topas-style)
      3. Hvis intet: AI-fallback
      4. Sidste fallback: tom

    extractionMethod logger hvilken path virkede.
    """
    result_count: Optional[int] = None
    result_summary = ""
    method = "none"

    if not markdown or len(markdown.strip()) < 50:
        return {"mealsCount": None, "mealsSummary": "", "extractionMethod": "empty"}

    operator = _detect_operator(url, markdown)

    extractor_map = {
        "smilrejser": _extract_smilrejser,
        "stjernegaard": _extract_stjernegaard,
        "viktorsfarmor": _extract_viktorsfarmor,
        "ruby": _extract_ruby,
        "kipling": _extract_kipling,
        "jysk": _extract_mfa_per_day,
        "topas": _extract_mfa_per_day,
        "fyrholt": _extract_mfa_per_day,
        "vagabond": _extract_mfa_per_day,
    }
    if operator and operator in extractor_map:
        out = extractor_map[operator](markdown)
        if out is not None:
            result_count, result_summary = out
            method = f"regex:{operator}"

    if result_count is None:
        out = _extract_mfa_per_day(markdown)
        if out is not None:
            result_count, result_summary = out
            method = "regex:mfa-generic"

    if result_count is None:
        out = _ai_extract(markdown, model=model)
        if out is not None:
            result_count, result_summary = out
            method = "ai"

    return {
        "mealsCount": result_count if (isinstance(result_count, int) and result_count > 0) else None,
        "mealsSummary": result_summary,
        "extractionMethod": method,
    }
