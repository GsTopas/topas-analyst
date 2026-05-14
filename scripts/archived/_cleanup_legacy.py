"""
One-shot oprydning: fjern legacy data der ikke længere er relevant.

- NPAP (Nepal) — gammel testdata, aldrig kørt af brugeren
- Eventuelt andre tours efter request
- Tjekker også for duplikat-operatører på samme tour (gamle hardcoded TARGETS
  vs nye approved_competitor_targets bruger forskellige operator-navne)

Kører fra topas-scraper-mappen:
    python -m topas_scraper._cleanup_legacy

Det her sletter både:
  - tours-rækker (operator+tour_slug der competes_with den fjernede tur)
  - departures der hænger på de tours
  - approved_competitor_targets-rækker for den fjernede tur

Kør derefter `python -m topas_scraper.cli export` for at rebuilde dashboard.json,
ELLER tryk "Hent konkurrenternes afgange + detaljer" på en hvilken som helst
tour i Streamlit.
"""
from __future__ import annotations

from topas_scraper.db import connect as connect_snapshots
from topas_scraper import catalog_db


# Tours at slette helt fra snapshots.db
LEGACY_TOUR_CODES = ["NPAP"]


def _print_section(title: str) -> None:
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def diagnose() -> None:
    """Vis hvad der findes nu, før vi sletter noget."""
    snap = connect_snapshots()
    cat = catalog_db.connect()

    _print_section("snapshots.db — tours per tour-kode")
    rows = snap.execute(
        """
        SELECT
            COALESCE(tour_code, competes_with, '(ingen)') AS code,
            operator,
            tour_slug,
            tour_name,
            last_seen_run
        FROM tours
        ORDER BY code, operator
        """
    ).fetchall()
    for r in rows:
        d = dict(r)
        print(f"  {d['code']:6} {d['operator']:25} {d['tour_slug']:40} run={d['last_seen_run'][:8] if d['last_seen_run'] else '?'}")

    _print_section("approved_competitor_targets — per tour-kode")
    targets = catalog_db.list_approved_targets(cat)
    by_code = {}
    for t in targets:
        by_code.setdefault(t["topas_tour_code"], []).append(t)
    for code, lst in sorted(by_code.items()):
        print(f"  {code}: {len(lst)} godkendte konkurrenter")
        for t in lst:
            print(f"    - {t['operator']:25} {t.get('tour_name') or '(intet navn)'}")

    _print_section("ÆGTE DUBLETTER — samme operator+tour_code+tour_slug (case-insensitive)")
    # NOTE: Forskellige slugs = forskellige ture (legitim flere-tour-per-operatør).
    # Vi rammer kun dubletter hvor slug ER ens (potentielt med forskellig casing
    # eller mellemrum-håndtering på operator-navn).
    dups = snap.execute(
        """
        SELECT competes_with AS code,
               LOWER(REPLACE(operator, ' ', '')) AS op_norm,
               LOWER(tour_slug) AS slug_norm,
               COUNT(*) AS n,
               GROUP_CONCAT(operator || ' / ' || tour_slug, ' | ') AS variants
        FROM tours
        WHERE competes_with IS NOT NULL
        GROUP BY competes_with, op_norm, slug_norm
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    if not dups:
        print("  Ingen ægte dubletter")
    for r in dups:
        d = dict(r)
        print(f"  {d['code']} · {d['op_norm']} ({d['n']} entries):")
        print(f"    {d['variants']}")

    snap.close()
    cat.close()


def cleanup_legacy_tours() -> None:
    """Slet legacy-tours og deres afgange + targets."""
    snap = connect_snapshots()
    cat = catalog_db.connect()

    for code in LEGACY_TOUR_CODES:
        _print_section(f"Sletter {code}")

        # 1) Find tours der competes_with denne kode (Topas-rækken + konkurrenter)
        tours_to_delete = snap.execute(
            "SELECT operator, tour_slug FROM tours WHERE competes_with=? OR tour_code=?",
            (code, code),
        ).fetchall()
        print(f"  Fjerner {len(tours_to_delete)} tour-rækker fra snapshots.db")

        # 2) Slet departures for de tours
        for r in tours_to_delete:
            snap.execute(
                "DELETE FROM departures WHERE operator=? AND tour_slug=?",
                (r["operator"], r["tour_slug"]),
            )

        # 3) Slet selve tours-rækkerne
        snap.execute("DELETE FROM tours WHERE competes_with=? OR tour_code=?", (code, code))

        # 4) Slet approved_competitor_targets
        n_targets = cat.execute(
            "DELETE FROM approved_competitor_targets WHERE topas_tour_code=?",
            (code,),
        ).rowcount
        print(f"  Fjerner {n_targets} godkendte targets fra catalog.db")

    snap.commit()
    cat.commit()
    snap.close()
    cat.close()


def cleanup_duplicate_operators() -> None:
    """Find ægte dubletter (samme slug, evt. med casing/mellemrum-forskel på
    operator-navn) og behold den med flest afgange.

    Forskellige slugs på samme operator+tour-kode betragtes IKKE som dubletter
    — de er typisk flere reelle ture (fx Smilrejser har både 'vandreferie' og
    'all-inclusive' Madeira-ture). Dem rører vi ikke."""
    _print_section("Rydder ægte dubletter (samme slug)")
    snap = connect_snapshots()

    dups = snap.execute(
        """
        SELECT competes_with AS code,
               LOWER(REPLACE(operator, ' ', '')) AS op_norm,
               LOWER(tour_slug) AS slug_norm
        FROM tours
        WHERE competes_with IS NOT NULL
        GROUP BY competes_with, op_norm, slug_norm
        HAVING COUNT(*) > 1
        """
    ).fetchall()

    removed = 0
    for r in dups:
        d = dict(r)
        rows = snap.execute(
            """
            SELECT operator, tour_slug,
                   (SELECT COUNT(*) FROM departures dep
                    WHERE dep.operator=tours.operator AND dep.tour_slug=tours.tour_slug) AS n_deps
            FROM tours
            WHERE competes_with=?
              AND LOWER(REPLACE(operator, ' ', ''))=?
              AND LOWER(tour_slug)=?
            ORDER BY n_deps DESC
            """,
            (d["code"], d["op_norm"], d["slug_norm"]),
        ).fetchall()

        keeper = rows[0]
        print(f"  {d['code']} · {keeper['operator']} / '{keeper['tour_slug']}' "
              f"→ behold ({keeper['n_deps']} afg.)")
        for r2 in rows[1:]:
            print(f"    × slet '{r2['operator']}' / '{r2['tour_slug']}' "
                  f"({r2['n_deps']} afg.)")
            snap.execute(
                "DELETE FROM departures WHERE operator=? AND tour_slug=?",
                (r2["operator"], r2["tour_slug"]),
            )
            snap.execute(
                "DELETE FROM tours WHERE operator=? AND tour_slug=?",
                (r2["operator"], r2["tour_slug"]),
            )
            removed += 1

    snap.commit()
    snap.close()
    print(f"  Fjernet {removed} dublet-rækker i alt")


def main() -> None:
    print("Topas — legacy cleanup")
    print()
    print("Før oprydning:")
    diagnose()

    print()
    print(f"VIL FJERNE: {', '.join(LEGACY_TOUR_CODES)} + duplikat-operatører")
    confirm = input("Fortsæt? (ja/nej): ").strip().lower()
    if confirm not in ("ja", "j", "yes", "y"):
        print("Afbrudt.")
        return

    cleanup_legacy_tours()
    cleanup_duplicate_operators()

    print()
    print("Efter oprydning:")
    diagnose()

    print()
    print("✓ Færdig. Kør nu enten:")
    print("    python -m topas_scraper.cli export")
    print("eller tryk 'Hent konkurrenternes afgange + detaljer' i Streamlit")
    print("for at rebuilde dashboard.json.")


if __name__ == "__main__":
    main()
