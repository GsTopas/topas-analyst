"""
Debug: Hvad ligner måltids-markører på Topas's tour-side nu?

Topas's _extract_mfa_per_day i meals.py forventer M/F/A-markører pr. dag,
fx "M/F/A", "M/-/-", "(M,F,A)", "(A)". Hvis Topas har redesigned siden,
vil regex'en ikke matche → tom data.

Usage:
    cd C:\\Users\\gs\\Downloads\\topas-scraper
    python debug_topas_meals.py
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
from topas_scraper.meals import extract_meals, _extract_mfa_per_day


URL = "https://www.topas.dk/vietnam-den-store-rundrejse-fra-syd-til-nord-ny-version/"


def main():
    print(f"Scraper {URL} ...")
    client = FirecrawlClient()
    result = client.scrape(URL, schema=TOUR_EXTRACTION_SCHEMA)
    if not result.success:
        print(f"FEJL: scrape fejlede — {getattr(result, 'error', 'no error')}")
        return

    md = result.markdown or ""
    print(f"Markdown længde: {len(md)} chars\n")

    # --- 1) Test eksisterende _extract_mfa_per_day ---
    print("=" * 70)
    print("Test af _extract_mfa_per_day() (nuværende Topas-extractor)")
    print("=" * 70)
    res = _extract_mfa_per_day(md)
    if res:
        total, summary = res
        print(f"  ✓ Match: {total} måltider — {summary}")
    else:
        print(f"  ✗ Ingen M/F/A-markører fundet i markdown.")

    # --- 2) Test extract_meals (full dispatch) ---
    print()
    print("=" * 70)
    print("Test af extract_meals() (operator-dispatch + AI fallback)")
    print("=" * 70)
    meals = extract_meals(md, url=URL)
    print(f"  mealsCount:        {meals.get('mealsCount')}")
    print(f"  mealsSummary:      {meals.get('mealsSummary')}")
    print(f"  extractionMethod:  {meals.get('extractionMethod')}")

    # --- 3) Find mulige måltids-markører i markdown ---
    print()
    print("=" * 70)
    print("Find måltids-keywords")
    print("=" * 70)
    for kw in ["morgenmad", "frokost", "middag", "aftensmad", "måltid",
               "M/F/A", "(M)", "(F)", "(A)", " M ", " F ", " A "]:
        positions = [m.start() for m in re.finditer(re.escape(kw), md, re.IGNORECASE)]
        print(f"  '{kw}': {len(positions)} forekomster")
        for pos in positions[:3]:
            ctx = md[max(0, pos - 60):pos + 100].replace("\n", " ⏎ ")
            print(f"     ...{ctx}...")

    # --- 4) Find dagsprogram-sektioner ("Dag 1", "Dag 2", ...) ---
    print()
    print("=" * 70)
    print("Find 'Dag N'-sektioner og dump de første ~600 chars af hver")
    print("=" * 70)
    dag_pattern = re.compile(r"(Dag\s+\d+(?:[\-—–]\d+)?\b)", re.IGNORECASE)
    matches = list(dag_pattern.finditer(md))
    print(f"  Antal 'Dag N'-headers: {len(matches)}")
    for i, m in enumerate(matches[:3]):  # første 3
        start = m.start()
        end = min(len(md), matches[i + 1].start() if i + 1 < len(matches) else start + 600)
        section = md[start:end][:600]
        print(f"\n  --- {m.group()} ---")
        print(section)

    # --- 5) Test forskellige potentielle nye mønstre ---
    print()
    print("=" * 70)
    print("Test alternative måltids-mønstre")
    print("=" * 70)
    patterns = [
        (r"\bM\/F\/A\b", "Klassisk M/F/A"),
        (r"\(M\)|\(F\)|\(A\)", "Single-letter parens"),
        (r"\bMorgen\b|\bFrokost\b|\bMiddag\b", "Standalone kapitaliseret"),
        (r"^[\-\*]\s+(Morgenmad|Frokost|Middag|Aftensmad)", "Bullet (Stjernegaard-style)"),
        (r"Inkluderet:\s*([^\n]+)", "Inkluderet: ...-linje"),
        (r"(\d+)\s*(morgenmad|frokost|middag|aftensmad)", "N word-tæller"),
    ]
    for pat, label in patterns:
        matches = re.findall(pat, md, re.MULTILINE | re.IGNORECASE)
        print(f"  {label}: {len(matches)} matches")
        if matches:
            for x in matches[:5]:
                print(f"     {x!r}")


if __name__ == "__main__":
    main()
