"""
Debug: scrape VNSN's Topas-side direkte og dump rå output, så vi kan se
hvorfor extraction returnerer tomt.

Kører fuld scrape-cyklus mod VNSN:
1. Henter URL fra topas_catalog
2. Kalder FirecrawlClient.scrape med fuld schema-extraction
3. Printer resultat: success-status, length af markdown, hvad LLM extraherede

Usage:
    cd C:\\Users\\gs\\Downloads\\topas-scraper
    python debug_vnsn_scrape.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Load .env eksplicit (Streamlit gør det automatisk, standalone-scripts skal selv)
from dotenv import load_dotenv
load_dotenv()

from topas_scraper.client import FirecrawlClient
from topas_scraper.db import connect, fetch_topas_catalog
from topas_scraper.extraction_schema import TOUR_EXTRACTION_SCHEMA


def main():
    # 1. Find URL i topas_catalog
    snap = connect()
    rows = fetch_topas_catalog(snap)
    snap.close()
    vnsn = next((dict(r) for r in rows if dict(r).get("tour_code") == "VNSN"), None)
    if vnsn is None:
        print("FEJL: VNSN ikke fundet i topas_catalog")
        return
    url = vnsn.get("url")
    print(f"VNSN katalog-data:")
    print(f"  tour_code:    {vnsn.get('tour_code')}")
    print(f"  tour_name:    {vnsn.get('tour_name')}")
    print(f"  country:      {vnsn.get('country')}")
    print(f"  duration_days: {vnsn.get('duration_days')}")
    print(f"  from_price:   {vnsn.get('from_price_dkk')}")
    print(f"  url:          {url}")
    print()

    if not url:
        print("FEJL: ingen URL i katalog")
        return

    # 2. Kald Firecrawl
    print("Forbinder til Firecrawl...")
    client = FirecrawlClient()
    print(f"Scraper {url} ...")
    print(f"(Dette tager 10-30 sekunder)")
    print()

    result = client.scrape(url, schema=TOUR_EXTRACTION_SCHEMA)

    # 3. Diagnostik
    print("=" * 70)
    print("RESULTAT")
    print("=" * 70)
    print(f"success:       {result.success}")
    print(f"error:         {getattr(result, 'error', None)}")
    md = getattr(result, "markdown", "") or ""
    print(f"markdown len:  {len(md)} chars")
    if md:
        print(f"markdown[:500]:")
        print("-" * 70)
        print(md[:500])
        print("-" * 70)
    print()
    print(f"extracted (Firecrawl JSON-LLM-extraction):")
    extracted = getattr(result, "extracted", None) or {}
    if not extracted:
        print("  (tom — Firecrawl LLM returnerede intet matchende schema)")
    else:
        print(json.dumps(extracted, indent=2, ensure_ascii=False)[:3000])

    # 4. Kør resten af pipelinen — parser → upsert → replace_departures → re-export
    print()
    print("=" * 70)
    print("PIPELINE-TEST (parser → DB → export)")
    print("=" * 70)

    from topas_scraper.config import load_active_targets
    from topas_scraper.parsers import PARSERS
    from topas_scraper.db import (
        connect as connect_db,
        start_run,
        finish_run,
        upsert_tour,
        replace_departures,
    )
    from topas_scraper.export import export

    targets = [t for t in load_active_targets("VNSN") if t.operator == "Topas"]
    if not targets:
        print("FEJL: ingen Topas-target fundet for VNSN i load_active_targets")
        return
    target = targets[0]
    print(f"target.operator = {target.operator}")
    print(f"target.parser_key = {target.parser_key}")
    print(f"target.tour_name = {target.tour_name}")
    print(f"target.tour_code = {target.tour_code}")
    print(f"target.competes_with = {target.competes_with}")
    print(f"target.country = {target.country}")
    print()

    parser = PARSERS.get(target.parser_key)
    print(f"Parser: {parser}")
    try:
        tour_dict, departures = parser(result, target)
    except Exception as e:
        print(f"FEJL i parser: {e}")
        import traceback; traceback.print_exc()
        return

    print(f"\nparser → tour_dict:")
    print(json.dumps(tour_dict, indent=2, ensure_ascii=False, default=str))
    print(f"\nparser → departures: {len(departures)} stk")
    for d in departures[:3]:
        print(f"  {d}")
    if len(departures) > 3:
        print(f"  ... og {len(departures) - 3} flere")

    if not departures:
        print("\nFEJL: parser returnerede 0 afgange. Pipeline ville stoppe her.")
        return

    # Kør meals-extraction lige som runner.py gør
    print()
    print("Kører meals-extraction ...")
    from topas_scraper.meals import extract_meals
    md = getattr(result, "markdown", "") or ""
    if md:
        meals = extract_meals(md, url=target.url)
        print(f"  meals: count={meals.get('mealsCount')}  summary={meals.get('mealsSummary')}  method={meals.get('extractionMethod')}")
        if meals.get("mealsCount") is not None:
            tour_dict["meals_included"] = meals["mealsCount"]
        if meals.get("mealsSummary"):
            tour_dict["meals_description"] = meals["mealsSummary"]

    # Skriv til DB ligesom runner.py gør
    print()
    print("Skriver til snapshots.db ...")
    conn = connect_db()
    run_id = start_run(conn, target_count=1)
    print(f"  run_id = {run_id}")
    upsert_tour(conn, tour_dict, run_id)
    dep_count = replace_departures(
        conn, target.operator, tour_dict["tour_slug"], departures, run_id
    )
    finish_run(conn, run_id, 1)
    conn.close()
    print(f"  upsert_tour OK, replace_departures returnerede {dep_count}")

    # Re-export
    print("\nRe-exporter dashboard.json ...")
    out = export()
    print(f"  Eksporteret: {out}")
    print()
    print("Færdig. Tjek nu dashboard.json — VNSN bør have departures udfyldt.")


if __name__ == "__main__":
    main()
