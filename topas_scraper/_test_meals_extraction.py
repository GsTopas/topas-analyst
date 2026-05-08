"""
Test meal-extraction MOD de gemte markdown-filer fra _research_meals.

Dette scrape'er IKKE — det læser bare outputs/meals_research/<operator>.md
og kører extract_meals() på hver. Sådan kan vi iterere på regex/prompt-logikken
uden at bruge Firecrawl-credits.

Kør lokalt:
    cd C:\\Users\\gs\\Downloads\\topas-scraper
    python -m topas_scraper._test_meals_extraction
"""
from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from topas_scraper.meals import extract_meals


# Match URLs til de markdown-filer vi gemte
TESTS = [
    ("Smilrejser",      "https://smilrejser.dk/portugal/vandreferie-paa-madeira"),
    ("JyskRejsebureau", "https://www.jysk-rejsebureau.dk/portugal/med-dansk-rejseleder/vandring-langs-levadaerne-paa-madeira/"),
    ("ViktorsFarmor",   "https://www.viktorsfarmor.dk/rejsemal/europa/portugal/vandreferie-pa-madeira"),
    ("RubyRejser",      "https://ruby-rejser.dk/vandreferie/frodige-madeira.html"),
    ("Stjernegaard",    "https://www.stjernegaard-rejser.dk/vietnam/rundrejser-vietnam/fra-nord-til-syd/priser-og-datoer/"),
    ("Albatros",        "https://www.albatros.dk/rejser/vidunderlige-vietnam"),
    ("Fyrholt",         "https://fyrholtrejser.dk/rejser/pyrenees-orientales/"),
    ("Kipling",         "https://www.kiplingtravel.dk/rejser/europa/tyrkiet/ararat-5165-meter"),
    ("Vagabond",        "https://www.vagabondtours.dk/tours/faellestur-via-francigena-i-toscana-2/"),
]


def main() -> None:
    research_dir = Path("outputs/meals_research")
    if not research_dir.exists():
        print("FEJL: outputs/meals_research/ findes ikke.")
        print("Kør først: python -m topas_scraper._research_meals")
        return

    print(f"{'Operatør':18} {'Method':22} {'Count':>6}  Summary")
    print("-" * 90)

    for op, url in TESTS:
        md_path = research_dir / f"{op}.md"
        if not md_path.exists():
            print(f"{op:18} (mangler {md_path.name})")
            continue

        md = md_path.read_text(encoding="utf-8")
        # Strip header line ("# Operator — URL\n\n") så det ikke forvirrer extractoren
        if md.startswith("# "):
            md = md.split("\n", 2)[-1] if md.count("\n") >= 2 else md

        result = extract_meals(md, url=url)
        method = result.get("extractionMethod", "?")
        count = result.get("mealsCount")
        summary = result.get("mealsSummary", "")
        count_str = str(count) if count is not None else "—"
        print(f"{op:18} {method:22} {count_str:>6}  {summary}")

    print()
    print("Method-værdier:")
    print("  regex:<operator>  — operator-specific regex matchede (BEDST)")
    print("  regex:mfa-generic — generic M/F/A counter matchede")
    print("  ai                — AI-extraction matchede")
    print("  none              — intet virkede (FEJL — undersøg)")


if __name__ == "__main__":
    main()
