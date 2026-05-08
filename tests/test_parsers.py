"""
Smoke tests for parsers.

These verify each parser produces *some* output on representative markdown
fixtures, doesn't crash, and respects the dataclass contract. They're not
end-to-end integration tests against live URLs — those happen via `cli scrape`.

To run:
    python -m pytest tests/  -v
"""

import sys
from pathlib import Path
from types import SimpleNamespace

# Allow imports without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from topas_scraper.parsers import topas, smilrejser, jysk, viktorsfarmor, ruby, nillesgislev
from topas_scraper.config import TARGETS


def make_scrape(markdown: str):
    """Build a minimal stand-in for ScrapeResult."""
    return SimpleNamespace(markdown=markdown, html=None, success=True, url="test://", title=None, status_code=200, error=None)


# Fixture: stripped-down markdown matching the shapes we observed during
# manual scraping for v0.5/v0.6.

TOPAS_FIXTURE = """
# Majestætiske tinder og levadavandring på Madeira
8 dage fra
9.970 DKK

#### Vælg din afgang

10\\. juli 2026
–
17\\. juli 2026

Få pladser
13.970 DKK
[Bestil Rejse](https://www.topas.dk/pages/checkout?tripDesignator=PTMD&tripCode=PTMD2605&tripMainCategory=2)

17\\. juli 2026
–
24\\. juli 2026

Garanteret afgang
13.470 DKK [Bestil Rejse](https://www.topas.dk/pages/checkout?tripDesignator=PTMD&tripCode=PTMD2606&tripMainCategory=2)
"""

SMILREJSER_FIXTURE = """
# Vandreferie på Madeira
Pris pr. person fra 12.995 DKK
8 dage

16.05.2026   8 dage   København   12.995 DKK   Udsolgt
29.07.2026   8 dage   København   12.995 DKK   +8 pladser
13.03.2027   8 dage   København   13.495 DKK   +8 pladser
"""

JYSK_FIXTURE = """
# Vandring langs levadaerne på Madeira
Varighed 8 dage
Frapris pr. pers. **Fra 13.950,-**

| Status | Rejseperiode | Udrejse | Tilmeldingsfrist | Rejseleder | Pris |
| --- | --- | --- | --- | --- | --- |
| På forespørgsel | 22.08.26 - 29.08.26 | København | | Rejseleder Thomas Lyhne | 13.950,- |
"""

VF_FIXTURE = """
# Vandreferie på Madeira
Priser fra
13.990 kr.
8 dages rejse

## Alle planlagte afgange

Dato

Pris

Rejseleder

Status

Afgang fra

**15\\. okt. 26**

8 dage

13.990 kr.

[![Lene Bach Larsen](https://www.viktorsfarmor.dk/media/abc/lene.jpg)\\
Lene Bach Larsen](https://www.viktorsfarmor.dk/rejseledere/lene-bach-larsen)

Garanteret

København

[Bestil](https://example.com)

**6\\. apr. 27**

8 dage

13.990 kr.

[![Hans-Jørgen Thougaard](https://www.viktorsfarmor.dk/media/xyz/hjt.jpg)\\
Hans-Jørgen Thougaard](https://www.viktorsfarmor.dk/rejseledere/hans-jorgen-thougaard)

Afventer pris

København

[Bestil](https://example.com)
"""

RUBY_FIXTURE = """
# Frodige Madeira
## Portugal | Gruppevandreferie med dansk vandreleder | Sværhedsgrad 2-3

**Travel Code**

VAG-053

[Fra 12.998,- DKK](https://ruby-rejser.dk/vandreferie/frodige-madeira.html#)

7 nætter

| #### Uge 48: Startdato 21.11.2026 varighed 7 nætter | [Fra 12.998,- DKK](https://example.com) |
| 2 personer Dobbeltværelse | 12.998,- DKK | [Bestil](https://example.com) |

**Turleder:**Karin Svane
"""

NG_FIXTURE = """
# Farverige Madeira
Fly

- **Varighed**

Nye afgange på vej

- **Pris fra**

Nye afgange på vej

8 dages rejse til Madeira med dansk rejseleder.
"""


def test_topas_parser():
    target = next(t for t in TARGETS if t.operator == "Topas")
    tour, deps = topas.parse(make_scrape(TOPAS_FIXTURE), target)
    assert tour["operator"] == "Topas"
    assert tour["tour_code"] == "PTMD"
    assert tour["from_price_dkk"] == 9970
    assert tour["duration_days"] == 8
    assert len(deps) == 2
    assert deps[0]["start_date"] == "2026-07-10"
    assert deps[0]["price_dkk"] == 13970
    assert deps[0]["availability_status"] == "Få pladser"
    assert deps[0]["departure_code"] == "PTMD2605"


def test_smilrejser_parser():
    target = next(t for t in TARGETS if t.operator == "Smilrejser")
    tour, deps = smilrejser.parse(make_scrape(SMILREJSER_FIXTURE), target)
    assert tour["operator"] == "Smilrejser"
    assert tour["from_price_dkk"] == 12995
    assert len(deps) == 3
    udsolgt = [d for d in deps if d["availability_status"] == "Udsolgt"]
    assert len(udsolgt) == 1
    assert udsolgt[0]["start_date"] == "2026-05-16"


def test_jysk_parser():
    target = next(t for t in TARGETS if t.operator == "Jysk Rejsebureau")
    tour, deps = jysk.parse(make_scrape(JYSK_FIXTURE), target)
    assert tour["operator"] == "Jysk Rejsebureau"
    assert tour["from_price_dkk"] == 13950
    assert tour["duration_days"] == 8
    assert len(deps) >= 1
    assert deps[0]["availability_status"] == "På forespørgsel"


def test_viktorsfarmor_parser():
    target = next(t for t in TARGETS if t.operator == "Viktors Farmor")
    tour, deps = viktorsfarmor.parse(make_scrape(VF_FIXTURE), target)
    assert tour["operator"] == "Viktors Farmor"
    assert tour["from_price_dkk"] == 13990
    # Viktors Farmor extraction is the most fragile because the JS-rendered
    # markdown shape varies. Smoke test: at least one departure should appear.
    assert len(deps) >= 1


def test_ruby_parser():
    target = next(t for t in TARGETS if t.operator == "Ruby Rejser")
    tour, deps = ruby.parse(make_scrape(RUBY_FIXTURE), target)
    assert tour["operator"] == "Ruby Rejser"
    assert tour["tour_code"] == "VAG-053"
    assert tour["from_price_dkk"] == 12998
    assert tour["duration_days"] == 8       # 7 nætter + 1
    assert len(deps) == 1
    assert deps[0]["start_date"] == "2026-11-21"
    assert deps[0]["price_dkk"] == 12998
    assert deps[0]["rejseleder_name"] == "Karin Svane"


def test_nillesgislev_parser():
    target = next(t for t in TARGETS if t.operator == "Nilles & Gislev")
    tour, deps = nillesgislev.parse(make_scrape(NG_FIXTURE), target)
    assert tour["operator"] == "Nilles & Gislev"
    # Per v0.6 finding: N&G shows "Nye afgange paa vej" — must detect this
    # and mark the tour as ineligible rather than fabricating departures.
    assert tour["fællesrejse_eligible"] is False
    assert "INELIGIBLE" in tour["eligibility_notes"]
    assert len(deps) == 0


def test_empty_input_does_not_crash():
    """All parsers must return ([], []) shaped output rather than raising."""
    target = next(t for t in TARGETS if t.operator == "Topas")
    empty = make_scrape("")
    for parser in [topas.parse, smilrejser.parse, jysk.parse, viktorsfarmor.parse, ruby.parse, nillesgislev.parse]:
        tour, deps = parser(empty, target)
        assert isinstance(tour, dict)
        assert isinstance(deps, list)


if __name__ == "__main__":
    # Allow running without pytest: python tests/test_parsers.py
    test_topas_parser()
    test_smilrejser_parser()
    test_jysk_parser()
    test_viktorsfarmor_parser()
    test_ruby_parser()
    test_nillesgislev_parser()
    test_empty_input_does_not_crash()
    print("✓ All parser smoke tests passed.")
