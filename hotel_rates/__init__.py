"""
hotel_rates — dedikeret tool til hotel-pris-benchmarking (Ilulissat-pilot).

Overvåger konkurrent-hotellers natpriser pr. kalenderdag via deres egne
booking-engines (deeplinks med check-in/check-out-datoer), scraper via
Firecrawl + samme tier-mønster som topas_scraper, og gemmer snapshots i
Supabase-tabellen hotel_rate_snapshots.

Kørsel:
    python -m hotel_rates.run --hotel hotel-arctic --start 2026-07-20 --nights 5
"""
