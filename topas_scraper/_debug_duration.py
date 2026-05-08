"""
Debug-script: scrape én Topas-tour og vis hvor AI'en læste duration fra.

Bruges til at undersøge når duration_days ser forkert ud i kataloget.
Output: rå markdown omkring 'dage'-omtaler + AI's extracted-resultat.

Kør:
    cd C:\\Users\\gs\\Downloads\\topas-scraper
    python -m topas_scraper._debug_duration https://www.topas.dk/cypern-aktiv-ferie-med-sol-strand-og-vandring/
"""
from __future__ import annotations

import sys
import re

from dotenv import load_dotenv
load_dotenv()

from topas_scraper.client import FirecrawlClient
from topas_scraper.topas_catalog import TOPAS_PRODUCT_METADATA_SCHEMA


def find_dage_contexts(md: str, window: int = 100) -> list[str]:
    """Find alle 'dage'-omtaler i markdown og returner ±window tegn rundt om hver."""
    md_l = md.lower()
    out = []
    for m in re.finditer(r"\b(\d+)\s*(dage?|nætter|n\.|nat\b)", md_l):
        start = max(0, m.start() - window)
        end = min(len(md), m.end() + window)
        snippet = md[start:end].strip()
        out.append(f"@ char {m.start()}: ...{snippet}...")
    return out


def main() -> None:
    if len(sys.argv) < 2:
        url = "https://www.topas.dk/cypern-aktiv-ferie-med-sol-strand-og-vandring/"
        print(f"(Ingen URL angivet — bruger default: {url})")
    else:
        url = sys.argv[1]

    client = FirecrawlClient()
    print(f"\nScraper {url}...")
    result = client.scrape(
        url,
        overrides={"only_main_content": False, "wait_for": 2000},
        schema=TOPAS_PRODUCT_METADATA_SCHEMA,
    )

    if not result.success:
        print(f"FEJL: {result.error}")
        return

    md = result.markdown or ""
    extracted = result.extracted or {}

    print("\n" + "=" * 70)
    print("AI EXTRACTED METADATA:")
    print("=" * 70)
    for k, v in extracted.items():
        print(f"  {k:18}: {v!r}")

    print("\n" + "=" * 70)
    print(f"ALLE 'X dage' / 'X nætter'-OMTALER I MARKDOWN ({len(md):,} tegn):")
    print("=" * 70)
    contexts = find_dage_contexts(md)
    for c in contexts[:30]:
        print(f"\n{c}\n")

    print("\n" + "=" * 70)
    print(f"Total contexts fundet: {len(contexts)}")


if __name__ == "__main__":
    main()
