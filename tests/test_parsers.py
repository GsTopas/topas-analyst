"""
Tests for den AKTIVE parse-pipeline (v0.8+): generic_ai (LLM-schema-extraction)
og jysk (eneste tilbageværende dedikerede regex-parser).

Historik: den gamle suite testede 8 legacy regex-parsere der blev pensioneret
i v0.8 og slettet i oprydningen 2026-08 — alle 7 tests fejlede desuden fordi
de slog op i config.TARGETS, som har været tom siden targets flyttede til DB
(load_active_targets). Fixtures konstrueres nu lokalt.

Kør:
    python -m pytest tests/ -v
"""

import sys
from pathlib import Path
from types import SimpleNamespace

# Allow imports without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from topas_scraper.config import TourTarget
from topas_scraper.parsers import PARSERS, generic_ai, jysk


def make_target(operator: str = "Testoperatør", parser_key: str = "generic_ai") -> TourTarget:
    return TourTarget(
        operator=operator,
        parser_key=parser_key,
        tour_name="Vandreferie på Madeira (test)",
        tour_code="TEST",
        url="https://example.com/rejser/madeira",
        competes_with="PTMD",
    )


def make_scrape(markdown: str = "", extracted: dict | None = None):
    """Minimal stand-in for client.ScrapeResult."""
    return SimpleNamespace(
        markdown=markdown, html=None, success=True, url="test://",
        title=None, status_code=200, error=None, extracted=extracted,
    )


# ---------------------------------------------------------------------------
# generic_ai — LLM-schema-pipelinen (bruges af ALLE operatører undtagen Jysk)
# ---------------------------------------------------------------------------

def test_generic_ai_basic_extraction():
    scrape = make_scrape(extracted={
        "tour_name": "Vandreferie på Madeira",
        "duration_days": 8,
        "from_price_dkk": 12995,
        "departures": [
            {"start_date": "2026-05-16", "price_dkk": 12995, "availability_status": "Udsolgt"},
            {"start_date": "2026-07-29", "price_dkk": 12995, "availability_status": "Åben"},
        ],
    })
    tour, deps = generic_ai.parse(scrape, make_target())
    assert isinstance(tour, dict) and isinstance(deps, list)
    assert tour["from_price_dkk"] == 12995
    assert tour["duration_days"] == 8
    assert len(deps) == 2
    assert deps[0]["start_date"] == "2026-05-16"  # sorteret kronologisk
    assert deps[0]["availability_status"] == "Udsolgt"


def test_generic_ai_nights_converted_to_days():
    # 7 nætter = 8 dage — konvertering sker i kode, ikke i LLM (CLAUDE.md-regel)
    scrape = make_scrape(extracted={"duration_nights": 7, "departures": []})
    tour, _ = generic_ai.parse(scrape, make_target())
    assert tour["duration_days"] == 8


def test_generic_ai_status_normalization():
    scrape = make_scrape(extracted={"departures": [
        {"start_date": "2026-09-01", "price_dkk": 9999, "availability_status": "Ledig"},
        {"start_date": "2026-09-08", "price_dkk": 9999, "availability_status": "+8 pladser"},
        {"start_date": "2026-09-15", "price_dkk": 9999, "availability_status": "Garanteret afgang"},
    ]})
    _, deps = generic_ai.parse(scrape, make_target())
    by_date = {d["start_date"]: d["availability_status"] for d in deps}
    assert by_date["2026-09-01"] == "Åben"       # 'Ledig' → Åben
    assert by_date["2026-09-08"] == "Åben"       # '+N pladser' er ÅBEN, ikke Få pladser
    assert by_date["2026-09-15"] == "Garanteret"


def test_generic_ai_drops_invalid_rows():
    scrape = make_scrape(extracted={"departures": [
        {"start_date": "16.05.2026", "price_dkk": 9999, "availability_status": "Åben"},  # ikke-ISO dato
        {"start_date": "2026-06-01", "price_dkk": None, "availability_status": "Åben"},   # pris mangler + bookbar
        {"start_date": "2026-06-08", "price_dkk": None, "availability_status": "Udsolgt"},  # pris-null OK for Udsolgt
        {"start_date": "2026-06-15", "price_dkk": 11111, "availability_status": "Åben"},
    ]})
    _, deps = generic_ai.parse(scrape, make_target())
    dates = [d["start_date"] for d in deps]
    assert dates == ["2026-06-08", "2026-06-15"]


def test_generic_ai_range_endpoint_dropped_but_backtoback_kept():
    # Gjøa-mønstret: et nøgent endepunkt (start=ref.end_date, ingen egen
    # end_date, samme pris+status) er et LLM-duplikat og skal droppes...
    scrape = make_scrape(extracted={"departures": [
        {"start_date": "2026-09-14", "end_date": "2026-09-21", "price_dkk": 15000, "availability_status": "Åben"},
        {"start_date": "2026-09-21", "price_dkk": 15000, "availability_status": "Åben"},
    ]})
    _, deps = generic_ai.parse(scrape, make_target())
    assert [d["start_date"] for d in deps] == ["2026-09-14"]

    # ...men ÆGTE uge-efter-uge-afgange med egne ranges må IKKE slettes,
    # selvom B starter hvor A slutter med samme pris (regression for
    # _merge_range_duplicates-stramningen 2026-08).
    scrape2 = make_scrape(extracted={"departures": [
        {"start_date": "2026-09-14", "end_date": "2026-09-21", "price_dkk": 15000, "availability_status": "Åben"},
        {"start_date": "2026-09-21", "end_date": "2026-09-28", "price_dkk": 15000, "availability_status": "Åben"},
    ]})
    _, deps2 = generic_ai.parse(scrape2, make_target())
    assert [d["start_date"] for d in deps2] == ["2026-09-14", "2026-09-21"]


def test_generic_ai_empty_extraction_does_not_crash():
    for extracted in (None, {}, {"departures": None}, {"departures": "garbage"}):
        tour, deps = generic_ai.parse(make_scrape(extracted=extracted), make_target())
        assert isinstance(tour, dict)
        assert deps == []


# ---------------------------------------------------------------------------
# jysk — dedikeret regex-parser (LLM mappede 'Ledige pladser' forkert)
# ---------------------------------------------------------------------------

JYSK_FIXTURE = """
# Vandring langs levadaerne på Madeira
Varighed 8 dage
Frapris pr. pers. **Fra 13.950,-**

| Status | Rejseperiode | Udrejse | Tilmeldingsfrist | Rejseleder | Pris |
| --- | --- | --- | --- | --- | --- |
| På forespørgsel | 22.08.26 - 29.08.26 | København | | Rejseleder Thomas Lyhne | 13.950,- |
"""


def test_jysk_parser():
    target = make_target(operator="Jysk Rejsebureau", parser_key="jysk")
    tour, deps = jysk.parse(make_scrape(JYSK_FIXTURE), target)
    assert tour["operator"] == "Jysk Rejsebureau"
    assert tour["from_price_dkk"] == 13950
    assert tour["duration_days"] == 8
    assert len(deps) >= 1
    assert deps[0]["availability_status"] == "På forespørgsel"


def test_jysk_empty_input_does_not_crash():
    target = make_target(operator="Jysk Rejsebureau", parser_key="jysk")
    tour, deps = jysk.parse(make_scrape(""), target)
    assert isinstance(tour, dict)
    assert isinstance(deps, list)


# ---------------------------------------------------------------------------
# Dispatch-tabellen
# ---------------------------------------------------------------------------

def test_parsers_mapping():
    assert PARSERS["jysk"] is jysk.parse
    for key, fn in PARSERS.items():
        if key != "jysk":
            assert fn is generic_ai.parse, f"{key} skal bruge generic_ai"
