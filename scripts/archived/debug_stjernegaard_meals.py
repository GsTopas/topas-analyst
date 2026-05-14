"""
Debug: Hvad ligner måltids-sektionen på Stjernegaards Vietnam-side?

Henter siden via Firecrawl, leder efter måltids-keywords i markdown'en og
dumper kontekst så vi kan se hvilket format de bruger nu.

Nuværende regex'er i meals.py forventer:
    '12 x morgenmad', '9 x frokost', '6 x middag'

Hvis Stjernegaard har skiftet format (fx '12 morgenmade inkluderet'),
ser vi det her.

Usage:
    cd C:\\Users\\gs\\Downloads\\topas-scraper
    python debug_stjernegaard_meals.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from topas_scraper.client import FirecrawlClient
from topas_scraper.extraction_schema import TOUR_EXTRACTION_SCHEMA
from topas_scraper.meals import extract_meals, _extract_stjernegaard


URL = "https://www.stjernegaard-rejser.dk/vietnam/rundrejser-vietnam/det-bedste-af-vietnam/"
DAGSPROGRAM_URL = URL.rstrip("/") + "/dagsprogram/"


def main():
    client = FirecrawlClient()

    print(f"Scraper hoved-side: {URL}")
    main_result = client.scrape(URL, schema=TOUR_EXTRACTION_SCHEMA)
    if not main_result.success:
        print(f"FEJL: hoved-side scrape fejlede — {getattr(main_result, 'error', 'no error')}")
        return
    main_md = main_result.markdown or ""
    print(f"  hoved-side markdown: {len(main_md)} chars")

    print(f"\nScraper dagsprogram: {DAGSPROGRAM_URL}")
    dp_result = client.scrape(DAGSPROGRAM_URL)
    if not dp_result.success:
        print(f"FEJL: dagsprogram scrape fejlede — {getattr(dp_result, 'error', 'no error')}")
        return
    dp_md = dp_result.markdown or ""
    print(f"  dagsprogram markdown: {len(dp_md)} chars\n")

    # Brug dagsprogram til måltids-analyse — det er der måltiderne står
    md = dp_md
    print(f"Bruger dagsprogram-markdown ({len(md)} chars) til måltids-analyse\n")

    # --- 1) Test nuværende Stjernegaard-extractor ---
    print("=" * 70)
    print("Test af nuværende _extract_stjernegaard()")
    print("=" * 70)
    res = _extract_stjernegaard(md)
    if res:
        total, summary = res
        print(f"  ✓ Match: {total} måltider — {summary}")
    else:
        print(f"  ✗ Ingen match. Regex'en finder ikke '12 x morgenmad'-mønster.")

    print()
    print("=" * 70)
    print("Test af extract_meals() (operator-dispatch)")
    print("=" * 70)
    meals = extract_meals(md, url=URL)
    print(f"  mealsCount:        {meals.get('mealsCount')}")
    print(f"  mealsSummary:      {meals.get('mealsSummary')}")
    print(f"  extractionMethod:  {meals.get('extractionMethod')}")

    # --- 2) Find måltids-keywords i markdown ---
    print()
    print("=" * 70)
    print("Find 'morgenmad' / 'frokost' / 'middag' / 'måltid' i markdown")
    print("=" * 70)
    keywords = ["morgenmad", "frokost", "middag", "måltid", "all inclusive", "halvpension", "fuldpension"]
    for kw in keywords:
        positions = [m.start() for m in re.finditer(kw, md, re.IGNORECASE)]
        print(f"\n'{kw}': {len(positions)} forekomster")
        for pos in positions[:5]:  # max 5 kontekst-snippets per keyword
            ctx_start = max(0, pos - 80)
            ctx_end = min(len(md), pos + 120)
            ctx = md[ctx_start:ctx_end].replace("\n", " ⏎ ")
            print(f"  ...{ctx}...")

    # --- 3) Find tal foran måltids-ord ---
    print()
    print("=" * 70)
    print("Find tal-foran-måltidsord (forskellige mønstre)")
    print("=" * 70)
    patterns = [
        (r"(\d+)\s*x\s*(morgenmad|frokost|middag|måltid)", "Nuværende mønster: 'N x word'"),
        (r"(\d+)\s+(morgenmad|frokost|middag|måltid)", "'N word' (uden x)"),
        (r"(\d+)\s*[\-·•]\s*(morgenmad|frokost|middag)", "'N - word'"),
        (r"(morgenmad|frokost|middag)\s*[\:\-]\s*(\d+)", "'word: N'"),
        (r"(\d+)\s+(morgenmade?r?|frokoster?|middage)", "'N word(plural)'"),
    ]
    for pat, label in patterns:
        matches = re.findall(pat, md, re.IGNORECASE)
        print(f"\n  {label}: {len(matches)} matches")
        for m in matches[:8]:
            print(f"    {m}")

    # --- 4) Direkte tælling af kapitaliserede måltids-ord ---
    # Stjernegaard's dagsprogram lister måltider per dag som "Morgenmad · Frokost · Middag"
    # — uden tæller. Vi skal tælle hvor mange gange hvert ord står.
    print()
    print("=" * 70)
    print("Direkte tælling af kapitaliserede måltids-ord (dagsprogram-format)")
    print("=" * 70)
    for word in ["Morgenmad", "Frokost", "Middag"]:
        # Match som standalone word (ikke mid-sentence prosa)
        pattern = rf"\b{word}\b"
        matches = re.findall(pattern, md)
        print(f"  '{word}' (standalone): {len(matches)} forekomster")

    # --- 5) Dump 1000 chars rundt om første "Morgenmad" ---
    print()
    print("=" * 70)
    print("Kontekst omkring første 'Morgenmad' i dagsprogram")
    print("=" * 70)
    pos = md.find("Morgenmad")
    if pos >= 0:
        print(md[max(0, pos - 300):pos + 700])
    else:
        print("Ingen 'Morgenmad' fundet.")


if __name__ == "__main__":
    main()
