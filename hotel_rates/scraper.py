"""
Scrape én hotel-nat: byg deeplink med datoer, kør Firecrawl JSON-extraction.

Tier 1 only i pilotten — Hotel Arctic's SynXis renderer priser som tekst
Firecrawl kan læse. Tier 3 (vision) kobles på når vi knækker BookYourStay
og Best Western, der sandsynligvis kræver screenshot-fallback.
"""
from __future__ import annotations

import re
from datetime import date, timedelta

from topas_scraper.client import FirecrawlClient

from .config import HotelTarget
from .schema import RATE_EXTRACTION_SCHEMA


class NightResult:
    """Resultat for én nat. success=False betyder scrape-fejl (≠ udsolgt)."""

    def __init__(
        self,
        stay_date: date,
        success: bool,
        rooms: list[dict] | None = None,
        sold_out: bool = False,
        currency: str = "DKK",
        error: str | None = None,
    ):
        self.stay_date = stay_date
        self.success = success
        self.rooms = rooms or []
        self.sold_out = sold_out
        self.currency = currency
        self.error = error


def scrape_night(
    client: FirecrawlClient,
    hotel: HotelTarget,
    stay_date: date,
    wait_bump_ms: int = 0,
) -> NightResult:
    """Scrape prisen for én nat (check-in stay_date, check-ud dagen efter).

    wait_bump_ms lægges oven i hotellets wait_for — bruges af retry-logikken
    til at give render-flakede nætter mere tid i andet forsøg.
    """
    # arrive_offset_days kompenserer for engines der UTC-forskyder datoen
    # (BookYourStay viser arrivaldate minus 1 dag i Grønlands-tid).
    # date_format: ISO for de fleste; bestwestern.com kræver MM/DD/YYYY.
    arrive = stay_date + timedelta(days=hotel.arrive_offset_days)
    url = hotel.url_template.format(
        arrive=arrive.strftime(hotel.date_format),
        depart=(arrive + timedelta(days=1)).strftime(hotel.date_format),
    )

    overrides = dict(hotel.scrape_overrides or {})
    if wait_bump_ms:
        overrides["wait_for"] = overrides.get("wait_for", 8000) + wait_bump_ms
        # Firecrawl kræver waitFor <= timeout/2 — hæv timeout tilsvarende,
        # ellers afvises retry'et med Bad Request (set 13/7 ved 18s wait).
        overrides["timeout"] = overrides["wait_for"] * 2 + 5000

    result = client.scrape(
        url,
        overrides=overrides,
        schema=RATE_EXTRACTION_SCHEMA,
    )

    if not result.success:
        return NightResult(stay_date, success=False, error=result.error or "no content")

    extracted = result.extracted or {}
    rooms = extracted.get("rooms") or []
    sold_out = bool(extracted.get("sold_out"))
    currency = extracted.get("currency") or "DKK"

    # Dato-kanariefugl: arrive_offset_days er en statisk kompensation for
    # BookYourStay's UTC-forskydning. Hvis engine'en en dag retter sin bug,
    # ville alle priser stille forskydes én dag. Schemaet udtrækker derfor
    # den VISTE ankomstdato (aldrig URL'ens), og vi fejler natten ved mismatch
    # frem for at gemme forskudte data.
    shown = extracted.get("arrival_date_shown")
    if shown:
        shown_iso = str(shown).strip()[:10]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", shown_iso) and shown_iso != stay_date.isoformat():
            return NightResult(
                stay_date,
                success=False,
                error=(
                    f"dato-kanariefugl: siden viser ankomst {shown_iso}, "
                    f"forventede {stay_date.isoformat()} — tjek "
                    f"arrive_offset_days for {hotel.key}"
                ),
            )

    # Valider room-rækker defensivt — LLM-output er extern data.
    clean: list[dict] = []
    for room in rooms:
        name = room.get("room_name")
        price = room.get("price")
        status = room.get("availability_status")
        # Bool er subtype af int — eksplicit ekskluderet (jf. _safe_int-lektion)
        if not name or isinstance(price, bool) or not isinstance(price, int):
            continue
        if price <= 0 or price > 200_000:  # sanity-range for en hotelnat i DKK
            continue
        if status not in ("Åben", "Få pladser", "Udsolgt"):
            status = "Åben"
        clean.append({"room_name": name, "price": price, "availability_status": status})

    if not clean and not sold_out:
        return NightResult(
            stay_date,
            success=False,
            error="0 rooms extracted og ikke markeret udsolgt — sandsynligvis render-fejl",
        )

    return NightResult(
        stay_date,
        success=True,
        rooms=clean,
        sold_out=sold_out,
        currency=currency,
    )
