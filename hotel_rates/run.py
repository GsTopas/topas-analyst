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
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Callable, Optional

from dotenv import load_dotenv

load_dotenv()

from topas_scraper.client import FirecrawlClient  # noqa: E402

from .config import HOTELS  # noqa: E402
from .db import connect, fetch_nightly_min, insert_rates  # noqa: E402
from .scraper import scrape_night  # noqa: E402

ProgressCallback = Callable[[str], None]

# Firecrawl hobby-plan: 5 concurrent. Samme default som topas_scraper.runner.
DEFAULT_MAX_WORKERS = 5


def run_rate_scrape_many(
    hotel_keys: list[str],
    start: date,
    nights: int,
    on_progress: Optional[ProgressCallback] = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> str:
    """Scrape flere hoteller × nætter parallelt. Returnerer run_id.

    Bruges af Streamlit-sidens scrape-knap (on_progress → st.status) og kan
    også kaldes fra CLI. Mønster fra topas_scraper.runner: emit + DB-writes
    serialiseres via locks (psycopg2-forbindelsen er ikke thread-safe), og
    Streamlit's ScriptRunContext propageres til worker-threads så progress-
    output ikke crasher med NoSessionContext.

    Forbindelsen lukkes ikke her — i Streamlit-kontekst er den cached og
    delt på tværs af reruns (PgConnection.close er bevidst no-op).
    """
    emit_lock = threading.Lock()
    db_lock = threading.Lock()

    def emit(msg: str) -> None:
        if on_progress is not None:
            with emit_lock:
                on_progress(msg)

    targets = []
    for key in hotel_keys:
        hotel = HOTELS.get(key)
        if hotel is None:
            raise ValueError(f"Ukendt hotel-key {key!r}. Kendte: {sorted(HOTELS)}")
        if not hotel.enabled:
            emit(f"⚠ {hotel.name} springes over (ikke aktiveret): {hotel.notes}")
            continue
        targets.append(hotel)
    if not targets:
        raise ValueError("Ingen aktive hoteller at scrape")

    client = FirecrawlClient()
    conn = connect()
    run_id = uuid.uuid4().hex[:12]

    jobs = [
        (hotel, start + timedelta(days=i))
        for hotel in targets
        for i in range(nights)
    ]
    emit(
        f"Run {run_id} — {len(targets)} hoteller × {nights} nætter "
        f"= {len(jobs)} scrapes ({max_workers} workers)"
    )

    streamlit_ctx = None
    add_script_run_ctx = None
    try:
        from streamlit.runtime.scriptrunner import (  # noqa: PLC0415
            add_script_run_ctx,
            get_script_run_ctx,
        )
        streamlit_ctx = get_script_run_ctx()
    except Exception:
        pass

    def process_one(job: tuple) -> bool:
        if streamlit_ctx is not None and add_script_run_ctx is not None:
            try:
                add_script_run_ctx(threading.current_thread(), streamlit_ctx)
            except Exception:
                pass
        hotel, night = job
        # Booking-engines render-flaker af og til (tom prisliste trods ledige
        # værelser — verificeret på SynXis). Ét gen-forsøg fanger de fleste.
        result = scrape_night(client, hotel, night)
        if not result.success:
            emit(f"  {hotel.name} {night.isoformat()} ⟳ fejlede, prøver igen...")
            result = scrape_night(client, hotel, night)
        if not result.success:
            emit(f"  {hotel.name} {night.isoformat()} ✗ {result.error}")
            return False
        with db_lock:
            rows = insert_rates(
                conn, run_id, hotel.key, night, result.rooms, result.currency,
            )
        bookable = [
            r["price"] for r in result.rooms
            if r["availability_status"] != "Udsolgt"
        ]
        if bookable:
            emit(
                f"  {hotel.name} {night.isoformat()} ✓ "
                f"billigste {min(bookable)} {result.currency} ({rows} offers)"
            )
        else:
            emit(f"  {hotel.name} {night.isoformat()} UDSOLGT")
        return True

    ok = 0
    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(jobs)))) as ex:
        futures = [ex.submit(process_one, j) for j in jobs]
        for fut in as_completed(futures):
            try:
                if fut.result():
                    ok += 1
            except Exception as e:  # noqa: BLE001
                emit(f"  ✗ uventet fejl i worker: {type(e).__name__}: {e!r}")

    emit(f"Færdig: {ok}/{len(jobs)} nætter gemt")
    return run_id


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
