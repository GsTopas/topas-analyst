"""
Classifier — uses Claude API to classify each discovered tour.

For every tour:
1. Reads the Firecrawl-extracted facts (title, duration, prices, dates, etc.)
2. Loads classification_playbook.md as instruction context
3. Loads recent override-decisions from the feedback log
4. Asks Claude for a structured verdict: tour_format, primary_activity,
   audience_segment, difficulty, is_faellesrejse, confidence, reasoning
5. Returns JSON; caller stores in tour_classifications table

This is the layer where the system "thinks". It's also the most expensive
layer per call — caching matters. We cache on the content_hash of the source
markdown, so re-classifying an unchanged page is a no-op.

Cost estimate: ~2,000 input tokens + ~500 output tokens per call.
At Claude Sonnet 4 pricing (~$3 input / $15 output per million tokens):
~$0.014 per tour. A 100-tour batch costs ~$1.40.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


# Lazily import anthropic — keeps the package optional if user only wants
# to run discovery + Firecrawl extract.
def _get_anthropic_client():
    try:
        import anthropic
    except ImportError as exc:
        raise RuntimeError(
            "anthropic package not installed. "
            "Run: pip install anthropic"
        ) from exc

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not set. Add it to .env (locally) or to "
            "Streamlit secrets (cloud)."
        )
    return anthropic.Anthropic(api_key=api_key)


PLAYBOOK_PATH = Path(__file__).resolve().parent.parent / "classification_playbook.md"
METHODOLOGY_PATH = Path(__file__).resolve().parent.parent / "methodology.md"
TAXONOMY_PATH = Path(__file__).resolve().parent.parent / "taxonomy.md"

CLAUDE_MODEL = "claude-sonnet-4-20250514"


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

SYSTEM_PROMPT_TEMPLATE = """\
You are a classification analyst for Topas Travel's competitive pricing intelligence \
system. Your job: classify a single competitor tour against the project's taxonomy \
(taxonomy.md §2.3).

You will receive:
1. Factual extracted data about the tour (title, duration, prices, dates, etc.)
2. The page's raw text content
3. The current classification playbook with operator-specific rules (BINDING)
4. Recent human override decisions (reference for similar cases)

Your task: return a single JSON object with these fields:

{{
  "is_faellesrejse": true/false,         // Result of the 5-criteria test
  "tour_format": "Fællesrejse" | "Individuel" | "Privat gruppe",
  "primary_activity": one of: Vandring, Trekking, Højrute / hut-to-hut, Cykling, \
Mountainbike, Sejlads + vandring, Sejlads + cykling, Yoga + vandring, Langrend / ski, \
Bjergbestigning / topbestigning, Multi-aktivitet rundrejse, Kulturrundrejse,
  "audience_segment": "Åben" | "30-50 år" | "Familie" | "Senior" | "Andet",
  "difficulty_norm": 1-5 integer,
  "country": "DK country code or full name",
  "region": "specific region within country",
  "duration_days": integer,
  "confidence": 0.0-1.0 float,
  "reasoning": "2-4 sentence explanation in Danish covering: which Fællesrejse \
criteria pass/fail, what activity-evidence drove the choice, and any uncertainty"
}}

CRITICAL RULES:
- Apply playbook rules first. If a playbook rule covers this tour, follow it strictly.
- The 5-criteria Fællesrejse test (taxonomy §2.3) is binding. If ANY criterion fails, \
is_faellesrejse=false regardless of how the operator labels the product.
- If you can't determine a field with confidence > 0.5, lower the overall confidence \
and explain the gap in `reasoning`.
- Return ONLY the JSON object, no preamble or postscript.

=== CLASSIFICATION PLAYBOOK ===
{playbook}

=== RECENT HUMAN OVERRIDES (most recent first, max 10) ===
{recent_overrides}
"""


@dataclass
class ClassificationInput:
    operator: str
    tour_url: str
    title: Optional[str]
    duration_days: Optional[int]
    from_price_dkk: Optional[int]
    country: Optional[str]
    region: Optional[str]
    has_fixed_dates: Optional[bool]
    has_published_prices: Optional[bool]
    page_markdown: str   # the raw scraped content


def load_playbook() -> str:
    if not PLAYBOOK_PATH.exists():
        return "(no playbook found — using methodology defaults only)"
    return PLAYBOOK_PATH.read_text(encoding="utf-8")


def format_recent_overrides(decisions: list[dict[str, Any]]) -> str:
    """Format recent override decisions as compact context for the classifier."""
    if not decisions:
        return "(none yet)"

    lines = []
    for i, d in enumerate(decisions[:10], 1):
        action = d.get("action", "?")
        reason = (d.get("reason") or "").strip()
        if not reason:
            continue
        # Trim to keep token cost reasonable
        if len(reason) > 200:
            reason = reason[:200] + "..."
        lines.append(f"{i}. [{action}] {reason}")
    return "\n".join(lines) if lines else "(no overrides with reasons)"


def build_classification_messages(
    input_data: ClassificationInput,
    playbook: str,
    recent_overrides: list[dict[str, Any]],
) -> tuple[str, list[dict[str, Any]]]:
    """Build (system_prompt, messages) for the Claude API call."""
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        playbook=playbook,
        recent_overrides=format_recent_overrides(recent_overrides),
    )

    # Trim page markdown to keep cost in check. Most operator pages have
    # the relevant info in the first ~5,000 chars (title, intro, departures).
    page_excerpt = input_data.page_markdown
    if len(page_excerpt) > 8000:
        page_excerpt = page_excerpt[:8000] + "\n\n[...truncated...]"

    user_content = f"""\
Classify this competitor tour:

OPERATOR: {input_data.operator}
URL: {input_data.tour_url}

EXTRACTED FACTS:
- Title: {input_data.title or '(unknown)'}
- Duration: {input_data.duration_days or '?'} days
- Country: {input_data.country or '(unknown)'}
- Region: {input_data.region or '(unknown)'}
- From-price: {input_data.from_price_dkk or '(none published)'} DKK
- Has fixed departure dates: {input_data.has_fixed_dates}
- Has published prices: {input_data.has_published_prices}

PAGE CONTENT:
{page_excerpt}

Respond with the JSON object only.
"""

    messages = [{"role": "user", "content": user_content}]
    return system_prompt, messages


# ---------------------------------------------------------------------------
# Calling Claude
# ---------------------------------------------------------------------------

def classify_tour(
    input_data: ClassificationInput,
    recent_overrides: Optional[list[dict[str, Any]]] = None,
    playbook_override: Optional[str] = None,
) -> dict[str, Any]:
    """
    Run the classifier on a single tour.

    Returns the parsed JSON verdict. Raises RuntimeError if Claude API
    is unavailable or returns malformed JSON.
    """
    client = _get_anthropic_client()
    playbook = playbook_override if playbook_override is not None else load_playbook()
    system_prompt, messages = build_classification_messages(
        input_data, playbook, recent_overrides or []
    )

    response = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=1024,
        system=system_prompt,
        messages=messages,
    )

    # Extract text from response
    text_blocks = [b.text for b in response.content if hasattr(b, "text")]
    raw_text = "\n".join(text_blocks).strip()

    # Strip code fences if Claude added them
    if raw_text.startswith("```"):
        # remove first fence line and trailing fence
        lines = raw_text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw_text = "\n".join(lines).strip()

    try:
        verdict = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Classifier returned non-JSON. First 500 chars: {raw_text[:500]!r}"
        ) from exc

    return verdict


def playbook_version() -> str:
    """Compute a short hash of the current playbook for audit."""
    if not PLAYBOOK_PATH.exists():
        return "no-playbook"
    content = PLAYBOOK_PATH.read_bytes()
    return hashlib.sha256(content).hexdigest()[:8]


# ---------------------------------------------------------------------------
# Batch helper — used by the discovery → classify pipeline
# ---------------------------------------------------------------------------

def classify_batch(
    inputs: list[ClassificationInput],
    catalog_conn: sqlite3.Connection,
    on_progress: Optional[Any] = None,
) -> list[tuple[ClassificationInput, dict[str, Any]]]:
    """
    Classify a batch of tours. Reads recent overrides once, then iterates.
    Returns list of (input, verdict) pairs.
    """
    from .catalog_db import fetch_recent_decisions, insert_classification

    recent_overrides = fetch_recent_decisions(catalog_conn, limit=10, only_overrides=True)
    playbook = load_playbook()
    pb_version = playbook_version()

    results = []
    for i, inp in enumerate(inputs, 1):
        if on_progress:
            on_progress(f"[{i}/{len(inputs)}] Klassificerer {inp.operator} → {inp.title or inp.tour_url[:40]}")

        try:
            verdict = classify_tour(inp, recent_overrides=recent_overrides, playbook_override=playbook)
        except RuntimeError as exc:
            if on_progress:
                on_progress(f"  ✗ {exc}")
            continue

        # Persist
        insert_classification(catalog_conn, inp.operator, inp.tour_url, pb_version, verdict)
        results.append((inp, verdict))

    return results
