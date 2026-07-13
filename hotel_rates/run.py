"""
CLI-runner: scrape et dato-vindue for et hotel og gem i Supabase.

    python -m hotel_rates.run --hotel hotel-arctic --start 2026-07-20 --nights 5

Kører nætterne sekventielt (skånsomt mod booking-engines — 1 request pr.
nat med Firecrawl imellem). Parallelisering kan tilføjes som i
topas_scraper.runner når vinduet vokser til 60-90 dage.
"""
from __future__ import annotations

import argparse
import sys
import uuid
from datetime import date, timedelta

from dotenv import load_dotenv

load_dotenv()

from topas_scraper.client import FirecrawlClient  # noqa: E402

from .config import HOTELS  # noqa: E402
from .db import connect, fetch_nightly_min, insert_rates  # noqa: E402
from .scraper import scrape_night  # noqa: E402


def run_rate_scrape(hotel_key: str, start: date, nights: int) -> str:
    """Scrape `nights` nætter fra `start` og gem snapshots. Returnerer run_id."""
    hotel = HOTELS.get(hotel_key)
    if hotel is None:
        raise ValueError(f"Ukendt hotel-key {hotel_key!r}. Kendte: {sorted(HOTELS)}")
    if not hotel.enabled:
        raise ValueError(f"{hotel.name} er ikke aktiveret endnu: {hotel.notes}")

    client = FirecrawlClient()
    conn = connect()
    run_id = uuid.uuid4().hex[:12]
    print(f"Run {run_id} — {hotel.name} · {nights} nætter fra {start.isoformat()}")

    ok = 0
    for i in range(nights):
        night = start + timedelta(days=i)
        result = scrape_night(client, hotel, night)
        if not result.success:
            print(f"  {night.isoformat()}  ✗ {result.error}")
            continue
        rows = insert_rates(
            conn, run_id, hotel.key, night, result.rooms, result.currency,
        )
        ok += 1
        # Kun bookbare værelser tæller som pris — engines viser ofte udsolgte
        # værelser MED pris + 'Sold Out'-badge (BW, BookYourStay gør begge).
        bookable = [r["price"] for r in result.rooms if r["availability_status"] != "Udsolgt"]
        if bookable:
            print(
                f"  {night.isoformat()}  ✓ {rows} offers · "
                f"billigste {min(bookable)} {result.currency}"
            )
        else:
            print(f"  {night.isoformat()}  UDSOLGT ({rows} værelser, alle optaget)")

    print(f"\n{ok}/{nights} nætter gemt. Benchmark (billigste pr. nat):")
    for row in fetch_nightly_min(conn, hotel.key, run_id):
        price = row["min_price_dkk"]
        label = f"{price} DKK" if price is not None else "UDSOLGT"
        print(f"  {row['stay_date']}  {label}  ({row['offers']} offers)")

    conn._conn.close()  # scripts lukker eksplicit (PgConnection.close er no-op)
    return run_id


def main() -> int:
    parser = argparse.ArgumentParser(description="Hotel-rate scrape (pilot)")
    parser.add_argument("--hotel", required=True, help="fx hotel-arctic")
    parser.add_argument("--start", required=True, help="ISO-dato, fx 2026-07-20")
    parser.add_argument("--nights", type=int, default=5)
    args = parser.parse_args()

    run_rate_scrape(args.hotel, date.fromisoformat(args.start), args.nights)
    return 0


if __name__ == "__main__":
    sys.exit(main())
