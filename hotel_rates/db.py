"""
DB-lag for hotel_rate_snapshots (Supabase Postgres).

Tabellen er oprettet via Supabase-migration create_hotel_rate_snapshots —
vi kører IKKE DDL fra app-koden (samme princip som topas_scraper).
Genbruger topas_scraper._pg_conn så PgBouncer-health-check og Streamlit-
caching følger med gratis.
"""
from __future__ import annotations

from datetime import date

from topas_scraper._pg_conn import PgConnection, connect  # noqa: F401


def insert_rates(
    conn: PgConnection,
    run_id: str,
    hotel_key: str,
    stay_date: date,
    rooms: list[dict],
    currency: str,
) -> int:
    """Indsæt snapshot-rækker for én nat. Returnerer antal rækker.

    rooms: [{room_name, price, availability_status}, ...] fra extraction.
    En udsolgt nat (rooms=[] men sold_out=True hos kalderen) indsættes som
    én række med room_name=NULL, price_dkk=NULL, status='Udsolgt' — så
    kalenderen kan skelne 'udsolgt' fra 'ikke scrapet'.
    """
    try:
        if not rooms:
            conn.execute(
                "INSERT INTO hotel_rate_snapshots "
                "(run_id, hotel, stay_date, room_name, price_dkk, currency, availability_status) "
                "VALUES (?, ?, ?, NULL, NULL, ?, 'Udsolgt')",
                (run_id, hotel_key, stay_date.isoformat(), currency),
            )
            conn.commit()
            return 1

        count = 0
        for room in rooms:
            conn.execute(
                "INSERT INTO hotel_rate_snapshots "
                "(run_id, hotel, stay_date, room_name, price_dkk, currency, availability_status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    hotel_key,
                    stay_date.isoformat(),
                    room["room_name"],
                    room["price"],
                    currency,
                    room["availability_status"],
                ),
            )
            count += 1
        conn.commit()
        return count
    except Exception:
        # Rollback så én fejlet nat ikke efterlader den delte forbindelse i
        # 'aborted transaction' — ellers fejler alle efterfølgende nætter i
        # samme run (og sidens egen load på den cachede forbindelse).
        conn.rollback()
        raise


def fetch_nightly_min(conn: PgConnection, hotel_key: str, run_id: str) -> list[dict]:
    """Billigste tilgængelige pris pr. nat for et run — benchmark-linjen."""
    cur = conn.execute(
        "SELECT stay_date, "
        "       MIN(price_dkk) FILTER (WHERE availability_status != 'Udsolgt') "
        "         AS min_price_dkk, "
        "       COUNT(*) FILTER (WHERE price_dkk IS NOT NULL "
        "                        AND availability_status != 'Udsolgt') AS offers "
        "FROM hotel_rate_snapshots "
        "WHERE hotel = ? AND run_id = ? "
        "GROUP BY stay_date ORDER BY stay_date",
        (hotel_key, run_id),
    )
    return [dict(r) for r in cur.fetchall()]
