"""
Configuration: URLs to scrape and operator metadata.

This is the only place URL-and-operator data lives. Adding a new tour or
operator should be a one-line edit here, plus a parser module.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class TourTarget:
    """A single URL to scrape, with the metadata needed to dispatch to the right parser."""

    operator: str
    parser_key: str          # which parser module handles this URL
    tour_name: str
    tour_code: Optional[str] # Topas turkode if Topas; else operator-id
    url: str
    country: str = "Portugal"
    region: str = "Madeira"
    # Which Topas-tour does this target compete against? For Topas itself, this
    # equals tour_code. For competitors, this links them to the Topas-tour they
    # belong to in the pair-pool. Lets us filter TARGETS to "scrape only PTMD's
    # competitor set" for a per-tour live scrape.
    competes_with: Optional[str] = None
    # Per-target scrape options. Most operators use defaults; sites with cookie
    # walls or heavy client-side rendering need overrides. None = use defaults.
    scrape_overrides: Optional[dict] = None
    # Tier 3 fallback: if Tier 1 (Firecrawl markdown) returns 0 departures from
    # this target, automatically retry via screenshot + Claude vision. See
    # methodology section 7.bis. Costs ~$0.02 per fallback call. Only enable for
    # operators whose pages have JS-rendered departure tables that Firecrawl
    # cannot extract reliably.
    vision_fallback: bool = False


# === Project paths ===
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_DB_PATH = DATA_DIR / "snapshots.db"
DEFAULT_EXPORT_PATH = DATA_DIR / "dashboard.json"


# === Tour targets ===
TARGETS: list[TourTarget] = []
# DEPRECATED: hardcoded list. Source-of-truth is now approved_competitor_targets
# in catalog.db (managed via Streamlit's Review-kandidater page) plus
# topas_catalog (Topas's own URLs from the catalog refresh). Use
# load_active_targets() below to read the live list.



# === Domain → canonical operator display-name ===
# Bruges af load_active_targets() til at gå fra catalog.db's operator-feltet
# (typisk "viktorsfarmor.dk") til det navn vi gemmer i departures.operator.
# Skal holde sig synkroniseret med OPERATOR_META-nøglerne nedenfor.
_DOMAIN_TO_OPERATOR = {
    "viktorsfarmor.dk":       "Viktors Farmor",
    "viktorsfarmor":          "Viktors Farmor",
    "smilrejser.dk":          "Smilrejser",
    "smilrejser":             "Smilrejser",
    "stjernegaard.dk":        "Stjernegaard Rejser",
    "stjernegaard":           "Stjernegaard Rejser",
    "stjernegaardrejser":     "Stjernegaard Rejser",
    "stjernegaard-rejser":    "Stjernegaard Rejser",
    "jysk-rejsebureau.dk":    "Jysk Rejsebureau",
    "jyskrejsebureau.dk":     "Jysk Rejsebureau",
    "jysk-rejsebureau":       "Jysk Rejsebureau",
    "jyskrejsebureau":        "Jysk Rejsebureau",
    "rubyrejser.dk":          "Ruby Rejser",
    "ruby-rejser.dk":         "Ruby Rejser",
    "rubyrejser":             "Ruby Rejser",
    "ruby-rejser":            "Ruby Rejser",
    "albatros-travel.dk":     "Albatros Travel",
    "albatros.dk":            "Albatros Travel",
    "albatrostravel.dk":      "Albatros Travel",
    "albatros":               "Albatros Travel",
    "albatrostravel":         "Albatros Travel",
    "albatros-travel":        "Albatros Travel",
    "besttravel.dk":          "Best Travel",
    "best-travel.dk":         "Best Travel",
    "besttravel":             "Best Travel",
    "best-travel":            "Best Travel",
    "nillesgislev.dk":        "Nilles & Gislev",
    "nilles-gislev.dk":       "Nilles & Gislev",
    "nillesgislev":           "Nilles & Gislev",
    "gjoatours.dk":           "Gjøa Tours",
    "gjoa.dk":                "Gjøa Tours",
    "gjoa":                   "Gjøa Tours",
    "gjoatours":              "Gjøa Tours",
    "kiplingtravel.dk":       "Kipling Travel",
    "kipling-travel.dk":      "Kipling Travel",
    "kiplingtravel":          "Kipling Travel",
    "kipling-travel":         "Kipling Travel",
    "vagabondtours.dk":       "Vagabond Tours",
    "vagabond-tours.dk":      "Vagabond Tours",
    "vagabondtours":          "Vagabond Tours",
    "vagabond-tours":         "Vagabond Tours",
    "fyrholt.dk":             "Fyrholt Rejser",
    "fyrholtrejser.dk":       "Fyrholt Rejser",
    "fyrholt":                "Fyrholt Rejser",
    "fyrholtrejser":          "Fyrholt Rejser",
}


# === Operator metadata — taxonomy.md §2.11 ===
OPERATOR_META = {
    "Topas":              {"holding": None,                   "segment": "Aktive grupperejser"},
    "Smilrejser":         {"holding": "Aller Leisure",        "segment": "Kulturrejser, Europa"},
    "Stjernegaard Rejser":{"holding": "Aller Leisure",        "segment": "Rundrejser med dansk leder"},
    "Nilles & Gislev":    {"holding": "Aller Leisure",        "segment": "Bus + fly grupperejser"},
    "Nyhavn Rejser":      {"holding": "Aller Leisure",        "segment": "Luxury / skræddersyet"},
    "Viktors Farmor":     {"holding": None,                   "segment": "Familieejet — kultur og vandring"},
    "Vagabond Tours":     {"holding": None,                   "segment": "Aktive ture"},
    "Jysk Rejsebureau":   {"holding": None,                   "segment": "Skræddersyet, få faste afgange"},
    "Ruby Rejser":        {"holding": None,                   "segment": "Vandreferier i Europa"},
    "Albatros Travel":    {"holding": None,                   "segment": "Bredt katalog, dansk rejseleder"},
    "Best Travel":        {"holding": "Stena Line Travel Group", "segment": "Kulturrejser m. dansk rejseleder"},
    "Gjøa Tours":         {"holding": None,                   "segment": "Aktive grupperejser"},
    "Kipling Travel":     {"holding": None,                   "segment": "Trekking — overvejende individuel"},
}


# === Firecrawl scrape options ===
# only_main_content trims navigation/footers — keeps tokens low and content clean.
# JS rendering is on by default so Viktors Farmor's dynamic departure list will populate.
SCRAPE_FORMATS = ["markdown", "html"]
SCRAPE_ONLY_MAIN = True
SCRAPE_TIMEOUT_MS = 30_000


# === Active targets — DB-backed (replaces hardcoded TARGETS for runtime) ===

# Operators whose pages need Tier 3 vision fallback (JS-heavy or stubborn).
# Cost: ~$0.01-0.02 ekstra per scrape via Claude vision, men højere extraction-rate.
PARSER_KEYS_NEEDING_VISION = {
    "topas",         # Tung React + booking-widget
    "albatros",      # React-rendered tabs, mange tour-variants
    "stjernegaard",  # Måltider på /dagsprogram/-undersider
    "ruby",          # Departures i accordion-dropdown der kun renderer ved click
}


def load_active_targets(tour_code: Optional[str] = None) -> list["TourTarget"]:
    """Build TourTarget list from DB (replaces hardcoded TARGETS).

    Sources:
      - Topas's own tour URLs from snapshots.db topas_catalog
      - Approved competitor URLs from catalog.db approved_competitor_targets

    If tour_code is given, only returns targets where competes_with == tour_code.
    Otherwise returns ALL active targets across all Topas tours.
    """
    from .db import connect as connect_snapshots, fetch_topas_catalog
    from . import catalog_db

    targets: list[TourTarget] = []

    # 1) Topas's own pages from topas_catalog
    snap = connect_snapshots()
    for row in fetch_topas_catalog(snap):
        d = dict(row)
        code = d.get("tour_code")
        if not code:
            continue
        if tour_code and code != tour_code:
            continue
        if not d.get("url"):
            continue
        targets.append(TourTarget(
            operator="Topas",
            parser_key="topas",
            tour_name=d.get("tour_name") or code,
            tour_code=code,
            url=d["url"],
            country=d.get("country") or "",
            region=None,
            competes_with=code,
            vision_fallback=True,
        ))
    snap.close()

    # 2) Approved competitor URLs from catalog.db
    cat = catalog_db.connect()
    approved = catalog_db.list_approved_targets(cat, topas_tour_code=tour_code)
    cat.close()

    for a in approved:
        parser_key = a.get("parser_key") or "generic_ai"
        operator_label = a.get("operator") or ""
        # Domain → canonical display name. Falls back to title-case for unknown
        # domains. Eksplicit mapping er nødvendig fordi naive title-case ville
        # smelte "viktors farmor" sammen til "Viktorsfarmor" og forårsage
        # dobbelt-operator-rows i DB (jf. Viktorsfarmor vs Viktors Farmor bug).
        operator_label = _DOMAIN_TO_OPERATOR.get(
            operator_label.lower(),
            operator_label.replace(".dk", "").replace("-", " ").title(),
        )
        targets.append(TourTarget(
            operator=operator_label or a["operator"],
            parser_key=parser_key,
            tour_name=a.get("tour_name") or a["tour_url"],
            tour_code=None,
            url=a["tour_url"],
            country="",
            region=None,
            competes_with=a["topas_tour_code"],
            vision_fallback=parser_key in PARSER_KEYS_NEEDING_VISION,
        ))

    return targets

