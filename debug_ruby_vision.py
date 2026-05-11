"""
Debug: kør hele vision-pipelinen mod Ruby's Korsika-URL og:
  1. Gem screenshot lokalt som PNG så du kan ÅBNE og se hvad Firecrawl fanger
  2. Send til Claude vision og dump det rå svar
  3. Print parsed departures

Hvis screenshot er tom/forkert → Firecrawl's render er problemet
Hvis screenshot er rigtig MEN Claude returnerer 0 → prompt eller Claude-vurdering

Usage:
    cd C:\\Users\\gs\\Downloads\\topas-scraper
    python debug_ruby_vision.py
"""
from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from topas_scraper.client import FirecrawlClient
from topas_scraper.vision_extractor import VisionExtractor, VISION_PROMPT


URL = "https://ruby-rejser.dk/vandreferie/korsika-skonhedens-o.html"
SCREENSHOT_OUT = Path("debug_ruby_screenshot.png")
RAW_RESPONSE_OUT = Path("debug_ruby_claude_raw.txt")


def main():
    print(f"=== Debug Ruby vision for {URL} ===\n")
    client = FirecrawlClient()
    extractor = VisionExtractor(client)

    # 1) Capture screenshot via Firecrawl (samme som vision_extractor gør internt)
    print("Step 1: Henter screenshot via Firecrawl...")
    screenshot_b64 = extractor._capture_screenshot(URL, overrides=None)

    if not screenshot_b64:
        print("FEJL: Firecrawl returnerede intet screenshot.")
        print("  Sandsynligt: Ruby's site har anti-bot eller Firecrawl er nede.")
        return

    print(f"  Screenshot modtaget: {len(screenshot_b64)} base64-chars (~{len(screenshot_b64)*3//4} bytes raw)")

    # Decode og gem så vi visuelt kan inspicere
    try:
        raw_bytes = base64.b64decode(screenshot_b64)
        SCREENSHOT_OUT.write_bytes(raw_bytes)
        print(f"  ✓ Gemt: {SCREENSHOT_OUT.resolve()}")
        print(f"  → Åbn PNG'en og verificér at den viser ALLE afgange (Uge 21, 22, 38, 40, 41 osv.)")
    except Exception as e:
        print(f"  Kunne ikke gemme PNG: {e}")

    # 2) Call Claude vision
    print(f"\nStep 2: Sender til Claude vision...")
    raw_response = extractor._call_claude_vision(screenshot_b64)
    if not raw_response:
        print("FEJL: Claude returnerede intet eller crashede.")
        return

    print(f"  Rå svar fra Claude ({len(raw_response)} chars):")
    print("  " + "-"*60)
    for line in raw_response.split("\n"):
        print(f"  {line}")
    print("  " + "-"*60)

    RAW_RESPONSE_OUT.write_text(raw_response, encoding="utf-8")
    print(f"  ✓ Gemt: {RAW_RESPONSE_OUT.resolve()}")

    # 3) Parse + dump strukturen
    print(f"\nStep 3: Parser Claude's svar ...")
    parsed = extractor._parse_response(raw_response)
    print(f"  Antal afgange ekstraheret: {len(parsed)}")
    for d in parsed:
        print(f"    {d['start_date']}  {d['price_dkk']!r:>10}  {d['availability_status']}")

    # 4) Diagnose
    print("\n=== DIAGNOSE ===")
    if not parsed:
        # Tjek om Claude returnerede valid JSON men tom departures, eller om JSON-parsing fejlede
        if "departures" in raw_response.lower():
            print("→ Claude returnerede gyldig JSON men 0 afgange.")
            print("  Mulighed A: Screenshot viser ikke afgangslisten korrekt — åbn PNG og verificér.")
            print("  Mulighed B: Claude læser ikke 'Uge XX'-format som afgange — tjek prompt.")
        else:
            print("→ Claude returnerede ikke 'departures'-key — JSON-parse fejlede.")
            print("  Rå svar ovenfor — er der prose/markdown?")
    else:
        print(f"→ Vision fandt {len(parsed)} afgange. Cloud-versionen burde også gøre det.")
        print("  Hvis cloud stadig viser 0, så er deploy ikke færdig eller env-variabler er forskellige.")


if __name__ == "__main__":
    main()
