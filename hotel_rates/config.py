"""
Hotel-targets: hvilke hoteller vi benchmarker, og hvordan deres
booking-engines tager imod dato-deeplinks.

url_template har {arrive} og {depart} pladsholdere (ISO YYYY-MM-DD).
Én scrape = én nat (depart = arrive + 1 dag).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class HotelTarget:
    key: str                 # stabil nøgle, bruges i CLI + DB's hotel-kolonne
    name: str                # display-navn
    booking_engine: str      # hvilken engine (til fejlsøgning/tuning)
    url_template: str        # med {arrive}/{depart} ISO-dato-pladsholdere
    enabled: bool = True     # False = engine endnu ikke knækket
    scrape_overrides: Optional[dict] = None
    # Timezone-kompensation: BookYourStay parser arrivaldate som UTC-midnat
    # og viser den i Grønlands-tid (UTC-2) → dagen FØR. Verificeret 2026-07-09:
    # arrivaldate=2026-09-16 viser 'Arrival: September 15'. Offset +1 løser det.
    arrive_offset_days: int = 0
    # Dato-format i URL'en. Default ISO; bestwestern.com kræver MM/DD/YYYY.
    date_format: str = "%Y-%m-%d"
    notes: str = ""


# Verificeret 2026-07-09: SynXis-deeplink renderer DKK-priser direkte
# (test: arrive=2026-07-15 gav DKK 2,690 + DKK 3,295). wait_for er
# nødvendig — SynXis er en React-app der henter rates async.
HOTELS: dict[str, HotelTarget] = {
    "hotel-arctic": HotelTarget(
        key="hotel-arctic",
        name="Hotel Arctic",
        booking_engine="SynXis (Sabre)",
        url_template=(
            "https://be.synxis.com/?chain=34919&hotel=97238"
            "&arrive={arrive}&depart={depart}&adult=2&rooms=1&locale=en-US"
        ),
        scrape_overrides={"wait_for": 9000},
    ),
    # Knækket 2026-07-09 via JS-bundle-analyse: deeplinkService læser
    # 'arrivaldate' + 'lengthofstay' (fundet i main-*.js). Priser renderer
    # som '2,595.00 DKK'. NB: arrive_offset_days=1 pga. UTC-parse (se felt).
    "hvide-falk": HotelTarget(
        key="hvide-falk",
        name="Hotel Hvide Falk",
        booking_engine="BookYourStay",
        url_template=(
            "https://aac-bookyourstay.bookyourstay.eu/booking/hhf/room"
            "?arrivaldate={arrive}&lengthofstay=1"
        ),
        scrape_overrides={"wait_for": 10000},
        arrive_offset_days=1,
    ),
    # Knækket 2026-07-09: samme BookYourStay-engine som Hvide Falk (tenant
    # 'hif' i stedet for 'aac'/'hhf') — samme deeplink-params og samme
    # UTC-2 dato-forskydning. Verificeret: arrivaldate=2026-09-16 viser
    # 'Arrival: September 15' og priser 2,750/2,895/3,295 DKK.
    "hotel-icefiord": HotelTarget(
        key="hotel-icefiord",
        name="Hotel Icefiord",
        booking_engine="BookYourStay",
        url_template=(
            "https://hif-bookyourstay.bookyourstay.eu/booking/hif/room"
            "?arrivaldate={arrive}&lengthofstay=1"
        ),
        scrape_overrides={"wait_for": 10000},
        arrive_offset_days=1,
    ),
    # Knækket 2026-07-09 — men via bestwestern.COM, ikke .dk!
    # .dk's booking-path ignorerer dato-params (viser statiske fra-priser —
    # verificeret: vinterdato og ingen-dato gav identiske priser; 25 mis-
    # visende rækker fra run 79d36b2b0050 slettet fra DB).
    # .com-motoren ekkoer datoerne og er den live bookable engine.
    # NB: BW Ilulissat kører p.t. flade årsrater (jul 2026 = feb 2027) —
    # det er reel prissætning, ikke en fejl. Udsolgt-detektion + fremtidige
    # prisændringer fanges stadig dato-for-dato.
    "hotel-ilulissat": HotelTarget(
        key="hotel-ilulissat",
        name="Best Western Plus Hotel Ilulissat",
        booking_engine="Best Western central (bestwestern.com)",
        url_template=(
            "https://www.bestwestern.com/en_US/book/hotel-rooms.96100.html"
            "?checkinDate={arrive}&checkoutDate={depart}"
            "&numberOfAdults=2&numberOfRooms=1"
        ),
        scrape_overrides={"wait_for": 12000},
        date_format="%m/%d/%Y",
    ),
}


def active_hotels() -> list[HotelTarget]:
    """Targets der er klar til produktion (engine verificeret)."""
    return [h for h in HOTELS.values() if h.enabled]
