"""
Research-script: hvad står om måltider på hver konkurrent-side?

Scraper én URL pr. operatør via Firecrawl, dumper hele markdown'en til
outputs/meals_research/<operator>.md, og udtrækker de afsnit der nævner
måltider — så vi kan se per-operatør hvor og hvordan info findes.

Output:
  - outputs/meals_research/<operator>.md          (fuld markdown)
  - outputs/meals_research/<operator>_meals.txt   (kun måltids-relevante afsnit)
  - outputs/meals_research/SUMMARY.md             (overblik + AI's forslag pr. tour)

Kører lokalt fra topas-scraper-mappen:
    cd C:\\Users\\gs\\Downloads\\topas-scraper
    python -m topas_scraper._research_meals

Tager 30-60 sekunder. Kræver FIRECRAWL_API_KEY i .env (det har du).
ANTHROPIC_API_KEY hjælper med at validere AI-extraction (men ikke nødvendigt).
"""
from __future__ import annotations

import os
import re
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from topas_scraper.client import FirecrawlClient
from topas_scraper.meals import extract_meals


# Ét repræsentativt URL pr. operatør — valgt fordi de findes i godkendte targets.
# Alle er Madeira-vandring eller Vietnam-rundrejse, så outputtet bør indeholde
# måltidsinfo i én eller anden form.
RESEARCH_URLS = [
    ("Smilrejser",          "https://smilrejser.dk/portugal/vandreferie-paa-madeira"),
    ("JyskRejsebureau",     "https://www.jysk-rejsebureau.dk/portugal/med-dansk-rejseleder/vandring-langs-levadaerne-paa-madeira/"),
    ("ViktorsFarmor",       "https://www.viktorsfarmor.dk/rejsemal/europa/portugal/vandreferie-pa-madeira"),
    ("RubyRejser",          "https://ruby-rejser.dk/vandreferie/frodige-madeira.html"),
    ("Stjernegaard",        "https://www.stjernegaard-rejser.dk/vietnam/rundrejser-vietnam/fra-nord-til-syd/priser-og-datoer/"),
    ("Albatros",            "https://www.albatros.dk/rejser/vidunderlige-vietnam"),
    # Nye operatører — research for fremtidig regex-extraction
    ("Fyrholt",             "https://fyrholtrejser.dk/rejser/pyrenees-orientales/"),
    ("Kipling",             "https://www.kiplingtravel.dk/rejser/europa/tyrkiet/ararat-5165-meter"),
    ("Vagabond",            "https://www.vagabondtours.dk/tours/faellestur-via-francigena-i-toscana-2/"),
]


# Søgeord der signalerer måltidsinfo — bruges til at klippe relevante afsnit ud
MEAL_KEYWORDS = [
    "måltid", "måltider",
    "morgenmad", "morgenbuffet",
    "frokost",
    "aftensmad", "middag", "aftensbuffet",
    "halvpension", "fuldpension",
    "all inclusive", "all-inclusive",
    "fuldt forplej", "halvt forplej",
    "inkluderet pension", "drikkevarer",
    "kulinariske", "smagsoplevelser",
    "M/F/A", "MFA",  # Topas-stil
]


def find_meal_sections(md: str, max_chars: int = 4000) -> str:
    """Find afsnit der nævner måltidsord — returner samlet tekst."""
    if not md:
        return "(tom markdown)"

    md_lower = md.lower()
    matches = []
    for kw in MEAL_KEYWORDS:
        kw_l = kw.lower()
        start = 0
        while True:
            idx = md_lower.find(kw_l, start)
            if idx == -1:
                break
            # Klip ±300 tegn rundt om hver match
            ctx_start = max(0, idx - 300)
            ctx_end = min(len(md), idx + len(kw) + 300)
            snippet = md[ctx_start:ctx_end].strip()
            matches.append((idx, kw, snippet))
            start = idx + len(kw)

    if not matches:
        return "(intet måltids-relateret stof fundet)"

    # Dedup overlappende snippets — sortér efter position, slå sammen tæt-på-hinanden
    matches.sort()
    deduped: list[str] = []
    last_end = -10000
    for idx, kw, snippet in matches:
        if idx < last_end + 200:
            continue  # for tæt på sidste — antag dækket
        deduped.append(f"\n--- match: '{kw}' @ char {idx} ---\n{snippet}\n")
        last_end = idx
        if sum(len(d) for d in deduped) > max_chars:
            deduped.append(f"\n... (mere fundet, men afkortet ved {max_chars} tegn) ...")
            break

    return "".join(deduped)


def main() -> None:
    out_dir = Path("outputs/meals_research")
    out_dir.mkdir(parents=True, exist_ok=True)

    if not os.getenv("FIRECRAWL_API_KEY"):
        print("FEJL: FIRECRAWL_API_KEY mangler i .env")
        return

    client = FirecrawlClient()
    summary_lines: list[str] = [
        "# Måltidsdata-research per operatør\n",
        "Scrape kørt via Firecrawl. Per operatør:",
        "  - URL der blev scrapet",
        "  - Antal tegn i markdown",
        "  - AI-extraction resultat (mealsCount + mealsSummary)",
        "  - Filer hvor du kan se rå-output\n",
        "Se per-operatør filer for fulde detaljer.\n",
    ]

    for op, url in RESEARCH_URLS:
        print(f"\n=== {op} ===")
        print(f"  Scraper {url}")

        try:
            # only_main_content=False så vi får alt — også måltidstabeller
            # der kan ligge i sidebar / accordion / tab-paneler
            scrape = client.scrape(url, overrides={"only_main_content": False})
            md = scrape.markdown or ""
        except Exception as exc:  # noqa: BLE001
            print(f"  FEJL: {exc}")
            summary_lines.append(f"## {op}\n  ❌ Scrape fejlede: {exc}\n")
            continue

        # Dump fuld markdown
        full_path = out_dir / f"{op}.md"
        full_path.write_text(f"# {op} — {url}\n\n{md}", encoding="utf-8")
        print(f"  Markdown: {len(md):,} tegn → {full_path.name}")

        # Klip måltids-relevante afsnit ud
        meal_sections = find_meal_sections(md)
        meals_path = out_dir / f"{op}_meals.txt"
        meals_path.write_text(
            f"{op} — måltids-relevante afsnit\nURL: {url}\n\n{meal_sections}",
            encoding="utf-8",
        )

        # Kør AI-extraction (samme funktion runner bruger)
        ai_result = extract_meals(md)
        print(f"  AI-result: count={ai_result.get('mealsCount')}, summary={ai_result.get('mealsSummary')!r}")

        # Tilføj til samlet summary
        summary_lines.append(f"## {op}\n")
        summary_lines.append(f"  - URL: {url}")
        summary_lines.append(f"  - Markdown: {len(md):,} tegn")
        summary_lines.append(f"  - Filer: `{full_path.name}` (fuld), `{meals_path.name}` (måltids-snippets)")
        summary_lines.append(f"  - AI-extraction: count=`{ai_result.get('mealsCount')}`, summary=`{ai_result.get('mealsSummary')}`")

        # Markér om der overhovedet er måltidsord i markdown

        # Markér om der overhovedet er måltidsord i markdown
        kw_hits = sum(1 for kw in MEAL_KEYWORDS if kw.lower() in md.lower())
        summary_lines.append(f"  - Måltids-keywords i markdown: **{kw_hits}** unikke matches")
        summary_lines.append("")

    summary_path = out_dir / "SUMMARY.md"
    summary_path.write_text("\n".join(summary_lines), encoding="utf-8")

    print()
    print(f"✓ Færdig. Se {summary_path}")
    print(f"  Per-operatør filer i {out_dir}/")
    print()
    print("Send mig outputtet af SUMMARY.md + et par eksempler fra _meals.txt")
    print("filerne, så bygger jeg bedre extraction-strategier.")


if __name__ == "__main__":
    main()
