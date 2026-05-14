"""
Tier 3 — Claude vision fallback.

Used when Tier 1 (Firecrawl markdown extraction) returns 0 departures from an
operator we know has data. Per methodology section 0.2 (data acquisition
principle), the tool's job is to deliver data parity with the analyst's
browser. When Firecrawl can't render JS-heavy pages (Albatros's React-rendered
departure tabs, Stjernegaard's priser-og-datoer subpages), we fall back to:

    1. Firecrawl screenshot of the page (Firecrawl uses Playwright internally
       to render JS, then captures the rendered viewport as PNG)
    2. Send screenshot + structured prompt to Claude API
    3. Claude reads the visible departure table and returns JSON
    4. Parse JSON back to our standard departures format

Cost: ~$0.02 per call. Only invoked when Tier 1 returns 0 departures from an
operator marked with `vision_fallback=True` in TourTarget.

Why this approach over alternatives:
- Playwright-locally: doesn't run on Streamlit Cloud, harder to maintain
- Find each operator's JSON API: per-operator investigation, fragile
- Claude vision: works on any site visible in a browser, robust against
  HTML structure changes, uses existing Anthropic API access
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
from datetime import date
from typing import Optional

from anthropic import Anthropic

log = logging.getLogger(__name__)

from .client import FirecrawlClient


VISION_PROMPT = """You are looking at a screenshot of a Danish travel agency's departure-dates page.

Extract:
- tour_duration_value: the integer shown next to a duration label (e.g. 8 for "8 dage", 7 for "7 nætter", 1 for "1 uge", 10 for "10-dages rejse")
- tour_duration_unit: the exact unit word as written, lowercased. One of: "dage", "nætter", "uge", "uger", "days", "nights", "week", "weeks"
- departures: array of every visible departure

For tour_duration_value + tour_duration_unit, look in the tour header or near departures.
DO NOT do the math yourself — just report the raw value and the unit. Examples:
- "8 dage"             → value=8,  unit="dage"
- "7 nætter"           → value=7,  unit="nætter"
- "varighed 7 nætter"  → value=7,  unit="nætter"
- "1 uge"              → value=1,  unit="uge"
- "10-dages rejse"     → value=10, unit="dage"
If no duration label is visible, set BOTH tour_duration_value and tour_duration_unit to null.

For EACH departure row, return:
- start_date: ISO format YYYY-MM-DD
- price_dkk: integer in DKK, no thousands separators
- availability_status: one of EXACTLY these values:
    "Garanteret"      (afgang er garanteret/bekræftet)
    "Få pladser"      (få pladser tilbage / få pladser / "X pladser tilbage" where X <= 3)
    "Udsolgt"         (sold out)
    "Afventer pris"   (date set but price not yet published)
    "Åben"            (default — booking is open, no special status)

DANISH DATE FORMATS du kan møde — KONVERTÉR ALLE til YYYY-MM-DD:
  "14. mar. 2027"                      → 2027-03-14
  "29/10/2026"                         → 2026-10-29
  "23.05.2026"                         → 2026-05-23   (vigtigt: dansk DD.MM.YYYY, ikke MM.DD)
  "Startdato 16.05.2026"               → 2026-05-16   (prefix "Startdato" ignoreres)
  "Uge 22: Startdato 23.05.2026"       → 2026-05-23   (uge-prefix ignoreres)
  "16. maj 2026"                       → 2026-05-16
  "Afrejse 14/03/2027"                 → 2027-03-14

DANISH PRICE FORMATS — extract integer only:
  "Fra 11.998,- DKK"                   → 11998
  "23.998 kr."                         → 23998
  "Pris fra 24.990"                    → 24990
  "kr. 13.470"                         → 13470

STATUS-DETECTION (when no price is shown but row exists):
  "Udsolgt" på rækken                  → status="Udsolgt", price_dkk=null
  "Få pladser" / "Få ledige"           → status="Få pladser"
  "Garanteret afgang" / "Garanteret"   → status="Garanteret"
  "Pris kommer snart" / no price       → status="Afventer pris", price_dkk=null
  Ingen særstatus, kun pris            → status="Åben"

Many Danish travel sites use accordion-style listings. EACH bar/row is a separate
departure even if expanded data isn't visible. Look for repeating layout patterns
with a date + week number + price (or "Udsolgt"-tag).

Return ONLY valid JSON in this exact shape, no commentary, no markdown fences:

{"tour_duration_value": 7, "tour_duration_unit": "nætter", "departures": [
  {"start_date": "2026-05-16", "price_dkk": null, "availability_status": "Udsolgt"},
  {"start_date": "2026-05-23", "price_dkk": 11998, "availability_status": "Åben"},
  {"start_date": "2026-09-12", "price_dkk": 11498, "availability_status": "Åben"}
]}

If no departure rows are visible at all (page shows only intro/marketing text),
return: {"tour_duration_value": null, "tour_duration_unit": null, "departures": []}

Important:
- Include EVERY visible row, including ones marked "Udsolgt"
- If row has no price (Udsolgt or Afventer pris), set price_dkk to null but include the row
- Convert Danish month abbreviations: jan/feb/mar/apr/maj/jun/jul/aug/sep/okt/nov/dec
- Currency is always DKK
- Danish numeric date format DD.MM.YYYY is common — NOT MM.DD.YYYY (US format)
"""


# Danish month abbreviations → numeric month
_DA_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "maj": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "okt": 10, "nov": 11, "dec": 12,
    "januar": 1, "februar": 2, "marts": 3, "april": 4, "maj.": 5, "juni": 6,
    "juli": 7, "august": 8, "september": 9, "oktober": 10, "november": 11, "december": 12,
}


class VisionExtractor:
    """Wraps the Firecrawl screenshot + Claude vision pipeline.

    Usage:
        extractor = VisionExtractor(firecrawl_client, anthropic_api_key)
        departures = extractor.extract(url, scrape_overrides={...})
    """

    def __init__(
        self,
        firecrawl_client: FirecrawlClient,
        anthropic_api_key: Optional[str] = None,
        # Bemærk: Claude model-aliases ændres over tid. claude-sonnet-4-6 er
        # den nuværende stabile alias der peger på Sonnet 4.6 (oktober 2025).
        # Tidligere brugte vi "claude-sonnet-4-20250514" som blev deprecated.
        model: str = "claude-sonnet-4-6",
    ):
        key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY not set. Add it to .env to enable Tier 3 vision fallback."
            )
        self._anthropic = Anthropic(api_key=key)
        self._model = model
        self._firecrawl = firecrawl_client

    def extract(self, url: str, scrape_overrides: Optional[dict] = None) -> list[dict]:
        """Capture a screenshot of url and ask Claude vision to extract departures.

        Returns list of dicts with start_date, price_dkk, availability_status.
        Returns [] if no departures could be extracted (no error).

        Side-effect: gemmer extracted tour_duration_days på self.last_tour_duration_days
        så caller (runner.py) kan læse det og opdatere tour_dict["duration_days"].
        """
        self.last_tour_duration_days: Optional[int] = None
        screenshot_b64 = self._capture_screenshot(url, scrape_overrides)
        if not screenshot_b64:
            return []

        raw = self._call_claude_vision(screenshot_b64)
        if not raw:
            return []

        return self._parse_response(raw)

    # ---- Internal pipeline steps ----

    def _capture_screenshot(self, url: str, overrides: Optional[dict]) -> Optional[str]:
        """Use Firecrawl to take a screenshot of the page after JS renders.

        Firecrawl's "screenshot" format runs Playwright internally — same as
        what a real browser would render. We get a base64-encoded PNG back.

        Bemærk: Firecrawl SDK 4.x ændrede format-syntax. Tidligere brugte vi
        "screenshot@fullPage" som streng; nu skal det være et dict-objekt.
        Vi prøver dict-format først, falder tilbage til den gamle streng-form
        hvis SDK'en er en endnu ældre version.
        """
        opts = {
            "formats": [{"type": "screenshot", "fullPage": True}],
            "only_main_content": False,
            "wait_for": 4000,  # generous default — JS-heavy sites need time
            "timeout": 60000,
        }
        if overrides:
            # Inherit the operator's existing wait_for if it's longer
            if "wait_for" in overrides and overrides["wait_for"] > opts["wait_for"]:
                opts["wait_for"] = overrides["wait_for"]

        try:
            doc = self._firecrawl._client.scrape(url, **opts)
            screenshot = _get_attr_or_key(doc, "screenshot")
            if not screenshot:
                return None

            # Firecrawl returns either a URL (https://...) or base64 data depending
            # on SDK version. Handle both.
            if screenshot.startswith("data:image"):
                return screenshot.split(",", 1)[1]  # strip "data:image/png;base64,"
            if screenshot.startswith("http"):
                # Fetch the URL and base64-encode the bytes
                import urllib.request
                with urllib.request.urlopen(screenshot, timeout=30) as resp:
                    return base64.b64encode(resp.read()).decode("ascii")
            # Already base64?
            return screenshot
        except Exception:
            # Don't crash the whole scrape if vision fallback fails — just return None
            log.exception("vision: screenshot capture failed for %s", url)
            return None

    def _call_claude_vision(self, screenshot_b64: str) -> Optional[str]:
        """Send the screenshot to Claude and ask for structured departure data."""
        try:
            response = self._anthropic.messages.create(
                model=self._model,
                max_tokens=2000,
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": screenshot_b64,
                            },
                        },
                        {"type": "text", "text": VISION_PROMPT},
                    ],
                }],
            )
            # Extract the text portion of the response
            text_blocks = [
                block.text for block in response.content
                if hasattr(block, "text")
            ]
            return "\n".join(text_blocks) if text_blocks else None
        except Exception:
            log.exception("vision: Claude API call failed")
            return None

    def _parse_response(self, raw: str) -> list[dict]:
        """Parse Claude's JSON response into our standard departures format.

        Tolerates: markdown code fences, leading/trailing prose, single-line
        or pretty-printed JSON.
        """
        # Strip markdown fences if present
        cleaned = raw.strip()
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

        # Find the JSON object — Claude sometimes adds prose despite instructions
        match = re.search(r"\{[\s\S]*\}", cleaned)
        if not match:
            log.warning("vision: no JSON found in response: %s", raw[:200])
            return []

        try:
            data = json.loads(match.group(0))
        except json.JSONDecodeError:
            log.exception("vision: JSON parse error. Raw: %s", raw[:200])
            return []

        # Gem tour-level duration så caller kan opdatere tour_dict.
        #
        # Vi beder Claude returnere RÅ værdi + enhed (fx 7 + "nætter") og laver
        # konverteringen deterministisk her i koden — Claude kan ikke pålideligt
        # selv lave "+1" på nætter-tal, så vi flyttede arithmetikken ud af prompten.
        # Bagudkompat: ældre prompts returnerede tour_duration_days direkte;
        # respekter det hvis det stadig findes.
        legacy_days = data.get("tour_duration_days")
        try:
            if legacy_days is not None and int(legacy_days) > 0:
                self.last_tour_duration_days = int(legacy_days)
        except (TypeError, ValueError):
            pass

        raw_value = data.get("tour_duration_value")
        raw_unit = data.get("tour_duration_unit")
        try:
            if raw_value is not None and raw_unit:
                n = int(raw_value)
                unit = str(raw_unit).strip().lower().rstrip(".")
                # Konvertering: hvad operatøren skriver → totale rejsedage
                #   nætter/nights → +1 (en 7-nætters rejse = 8 dage inkl. ankomst+afrejse)
                #   dage/days     → uændret
                #   uge/week      → ×7
                #   uger/weeks    → ×7
                if unit in ("nætter", "nights", "natter"):
                    self.last_tour_duration_days = n + 1
                elif unit in ("dage", "days"):
                    self.last_tour_duration_days = n
                elif unit in ("uge", "week"):
                    self.last_tour_duration_days = n * 7
                elif unit in ("uger", "weeks"):
                    self.last_tour_duration_days = n * 7
        except (TypeError, ValueError):
            pass

        raw_departures = data.get("departures", [])
        if not isinstance(raw_departures, list):
            return []

        # Convert to standard departure dict format used by the rest of the system.
        # Accepterer null-pris for ikke-bookbare statuses (Udsolgt, Afventer pris) —
        # vi vil stadig gemme rækken så Markeds-kalenderen kan vise demand-signaler
        # selv uden pris (fx Ruby's "Uge 21: Udsolgt" uden synlig pris).
        result = []
        seen = set()
        NO_PRICE_OK = {"Udsolgt", "Afventer pris"}
        for d in raw_departures:
            if not isinstance(d, dict):
                continue
            iso_date = self._normalize_date(d.get("start_date"))
            price = self._normalize_price(d.get("price_dkk"))
            status = self._normalize_status(d.get("availability_status"))

            # Skip kun hvis dato mangler. Pris-null tolereres for ikke-bookbare rækker.
            if not iso_date:
                continue
            if price is None and status not in NO_PRICE_OK:
                # Pris null + bookbar status er suspekt — skip for at undgå hallucination
                continue
            if iso_date in seen:
                continue
            seen.add(iso_date)

            result.append({
                "departure_code": None,
                "start_date": iso_date,
                "end_date": None,
                "price_dkk": price,
                "availability_status": status,
                "flight_origin": "København",  # default — Claude doesn't reliably extract this
                "rejseleder_name": None,
            })

        result.sort(key=lambda x: x["start_date"])
        return result

    @staticmethod
    def _normalize_date(value) -> Optional[str]:
        """Accept ISO YYYY-MM-DD or various Danish formats; return ISO."""
        if not value or not isinstance(value, str):
            return None
        s = value.strip()

        # Already ISO?
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
        if m:
            try:
                return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
            except ValueError:
                return None

        # Danish "14. mar. 2027" or "14 mar 2027"
        m = re.match(r"^(\d{1,2})\.?\s+([a-zæøå.]+)\s+(\d{4})$", s, re.IGNORECASE)
        if m:
            day = int(m.group(1))
            month_str = m.group(2).lower().rstrip(".")
            year = int(m.group(3))
            month = _DA_MONTHS.get(month_str)
            if month:
                try:
                    return date(year, month, day).isoformat()
                except ValueError:
                    return None

        # Slash format "29/10/2026"
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
        if m:
            try:
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
            except ValueError:
                return None

        # Dot format "29.10.2026"
        m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", s)
        if m:
            try:
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
            except ValueError:
                return None

        return None

    @staticmethod
    def _normalize_price(value) -> Optional[int]:
        """Accept int or string like '23998' or '23.998' or '23,998'."""
        if value is None:
            return None
        if isinstance(value, int):
            return value if value > 1000 else None
        if isinstance(value, float):
            return int(value) if value > 1000 else None
        if isinstance(value, str):
            cleaned = re.sub(r"[^\d]", "", value)
            if cleaned:
                n = int(cleaned)
                return n if n > 1000 else None
        return None

    @staticmethod
    def _normalize_status(value) -> str:
        """Map Claude's status string to our enum. Fall back to Åben if unknown."""
        valid = {"Garanteret", "Få pladser", "Udsolgt", "Afventer pris", "Åben"}
        if value in valid:
            return value
        if not value or not isinstance(value, str):
            return "Åben"
        # Lenient match
        s = value.strip().lower()
        if "garant" in s:
            return "Garanteret"
        if "udsolg" in s or "sold out" in s:
            return "Udsolgt"
        if "få plad" in s or "limited" in s or "low" in s:
            return "Få pladser"
        if "afvent" in s or "pending" in s:
            return "Afventer pris"
        return "Åben"


def _get_attr_or_key(obj, name):
    """Mirror of client.py's helper — Firecrawl SDK returns either object or dict."""
    if obj is None:
        return None
    if hasattr(obj, name):
        return getattr(obj, name, None)
    if isinstance(obj, dict):
        return obj.get(name)
    return None
