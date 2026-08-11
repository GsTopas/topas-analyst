"""
Destination-overblik — konkurrence-positionering pr. destination.

Hovedformaal: hvordan ligger Topas ift. konkurrenterne paa en given
destination (Groenland, Italien, Spanien, ...) — SÆSON FOR SÆSON?
- Operator-sammendrag pr. saeson (ture, afgange, billigste, median, % udsolgt)
- Per-tur tabel med KUN den valgte saesons afgange og priser
- Ture uden offentliggjorte afgange samles i egen sektion (i stedet for at
  staa med 0/— i alle saeson-tabeller)

Destinations-bestemmelse pr. konkurrent-tur: scrapet country-felt hvis sat,
ellers udledt via competes_with → topas_catalog.country (Topas-tur-koden
konkurrenten er kortlagt til). Sidste led er robust over for at scraperen
ikke altid faar country med.

Saeson-inddeling af afgange (pr. start_date-maaned):
- Hoejsaeson: jun-aug (6,7,8)
- Mellemsaeson: feb-maj + sep (2,3,4,5,9)
- Lavsaeson: okt-jan (1,10,11,12)
Hver afgang hoerer til praecis ÉN saeson; en tur kan optraede i flere
saesoner med hver sin delmaengde af afgange. (Graenserne er kalibreret til
Groenland; juster SEASON_MONTHS hvis en destination har anden saesonprofil.)

Datakilde: tours + departures + topas_catalog (for Topas-ture uden tours-row).
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Destinationer · Topas", page_icon="🌍", layout="wide")

# Password-gate
from topas_scraper._auth import require_auth  # noqa: E402
require_auth()


# ---------------------------------------------------------------------------
# Saeson-definitioner
# ---------------------------------------------------------------------------

SEASON_HIGH = "🌞 Højsæson"
SEASON_MID = "❄️ Mellemsæson"
SEASON_LOW = "🌑 Lavsæson"
SEASONS = [SEASON_HIGH, SEASON_MID, SEASON_LOW]
SEASON_LABELS = {
    SEASON_HIGH: "Højsæson (jun-aug)",
    SEASON_MID: "Mellemsæson (feb-maj + sep)",
    SEASON_LOW: "Lavsæson (okt-jan)",
}
SEASON_MONTHS = {
    SEASON_HIGH: {6, 7, 8},
    SEASON_MID: {2, 3, 4, 5, 9},
    SEASON_LOW: {1, 10, 11, 12},
}
# Korte nøgler brugt i SQL-resultatet ('HIGH'/'MID'/'LOW')
_SEASON_KEY = {SEASON_HIGH: "HIGH", SEASON_MID: "MID", SEASON_LOW: "LOW"}


def _month_to_season(m: int) -> str:
    for s, months in SEASON_MONTHS.items():
        if m in months:
            return s
    return ""


def _season_case_sql(month_expr: str) -> str:
    """CASE-udtryk der mapper en maaned-kolonne til HIGH/MID/LOW.

    Bygget fra SEASON_MONTHS saa Python og SQL aldrig kan komme ud af sync.
    """
    high = ", ".join(str(m) for m in sorted(SEASON_MONTHS[SEASON_HIGH]))
    mid = ", ".join(str(m) for m in sorted(SEASON_MONTHS[SEASON_MID]))
    return (
        f"CASE WHEN {month_expr} IN ({high}) THEN 'HIGH' "
        f"WHEN {month_expr} IN ({mid}) THEN 'MID' "
        f"ELSE 'LOW' END"
    )


# ---------------------------------------------------------------------------
# Data-loading
# ---------------------------------------------------------------------------

# Stavevarianter pr. destination — scrapede country-felter er ikke
# normaliserede (Groenland/Greenland/Grønland optraeder alle i DB).
_COUNTRY_ALIASES: dict[str, set[str]] = {
    "grønland": {"grønland", "groenland", "greenland"},
    "færøerne": {"færøerne", "faeroerne", "faroe islands"},
}


def _aliases_for(dest: str) -> list[str]:
    d = dest.strip().lower()
    return sorted(_COUNTRY_ALIASES.get(d, {d}))


@st.cache_data(ttl=600)
def _load_destinations() -> list[tuple[str, int]]:
    """Liste af (land, antal Topas-ture) sorteret efter antal, faldende."""
    from topas_scraper._pg_conn import connect as pg_connect  # noqa: PLC0415

    conn = pg_connect()
    rows = conn.execute("""
        SELECT country AS land, COUNT(*) AS n
        FROM topas_catalog
        WHERE country IS NOT NULL AND country != ''
          AND (audience_segment IS NULL OR audience_segment != 'Udgået')
        GROUP BY country
        ORDER BY n DESC, country
    """).fetchall()
    conn.close()
    return [(r["land"], r["n"]) for r in rows]


@st.cache_data(ttl=600)
def _load_destination_data(dest: str) -> pd.DataFrame:
    """Hent destinationens ture med PER-SÆSON afgangs-aggregater.

    Returnerer én row pr. (tur, sæson-med-afgange). Ture helt uden
    kommende afgange faar én row med season=NULL. Kolonner:
    - operator, tour_slug, tour_name, url, duration_days, from_price_dkk,
      tour_format  (tur-metadata, gentaget pr. sæson-row)
    - season ('HIGH'/'MID'/'LOW' eller NULL)
    - afgange, min_pris, max_pris, udsolgt, faa, garanteret, aaben
      (KUN den sæsons afgange, næste 24 mdr)

    Fra-pris: scrapet tours-værdi med fallback til topas_catalog (LLM-
    extraction misser den ind imellem — kataloget har altid Topas-priser).
    """
    from topas_scraper._pg_conn import connect as pg_connect  # noqa: PLC0415

    aliases = _aliases_for(dest)
    ph = ", ".join("?" for _ in aliases)
    season_case = _season_case_sql("EXTRACT(MONTH FROM d.start_date::date)::int")

    conn = pg_connect()
    rows = conn.execute(f"""
        WITH dest_tours AS (
            SELECT DISTINCT
                t.operator,
                t.tour_slug,
                t.tour_name,
                t.url,
                t.duration_days,
                COALESCE(t.from_price_dkk, tc.from_price_dkk) AS from_price_dkk,
                t.tour_format
            FROM tours t
            LEFT JOIN topas_catalog tc ON tc.tour_code = t.competes_with
            -- Scrapet country vinder hvis sat; ellers landet fra den
            -- Topas-tur konkurrenten er kortlagt til. Eksakt IN-match
            -- (wildcard-LIKE fangede i sin tid "Graekenland" for Groenland).
            WHERE LOWER(COALESCE(NULLIF(t.country, ''), tc.country, ''))
                  IN ({ph})
        ),
        topas_extra AS (
            -- Topas-ture i topas_catalog som ikke har tours-row endnu.
            -- audience_segment='Individuel' = rejseforslag uden faste afgange
            -- (fx Topas's 7 individuelle Groenlandsrejser). Andre = Faellesrejse.
            SELECT
                'Topas' AS operator,
                NULL AS tour_slug,
                tc.tour_name,
                tc.url,
                tc.duration_days,
                tc.from_price_dkk,
                CASE
                    WHEN tc.audience_segment = 'Individuel' THEN 'Rejs på egen hånd'
                    ELSE 'Fællesrejse'
                END AS tour_format
            FROM topas_catalog tc
            WHERE LOWER(tc.country) IN ({ph})
              AND (tc.audience_segment IS NULL OR tc.audience_segment != 'Udgået')
              AND NOT EXISTS (
                  SELECT 1 FROM tours t
                  WHERE t.operator = 'Topas' AND t.url = tc.url
              )
        ),
        all_tours AS (
            SELECT * FROM dest_tours
            UNION ALL
            SELECT * FROM topas_extra
        ),
        season_stats AS (
            SELECT
                d.operator,
                d.tour_slug,
                {season_case} AS season,
                COUNT(*) AS afgange,
                MIN(d.price_dkk)::int AS min_pris,
                MAX(d.price_dkk)::int AS max_pris,
                SUM(CASE WHEN d.availability_status = 'Udsolgt' THEN 1 ELSE 0 END) AS udsolgt,
                SUM(CASE WHEN d.availability_status = 'Få pladser' THEN 1 ELSE 0 END) AS faa,
                SUM(CASE WHEN d.availability_status = 'Garanteret' THEN 1 ELSE 0 END) AS garanteret,
                SUM(CASE WHEN d.availability_status = 'Åben' THEN 1 ELSE 0 END) AS aaben
            FROM departures d
            -- 24 mdr vindue saa vi fanger naeste aars saeson uanset hvornaar
            -- man kigger. Hver afgang taeller i praecis ÉN saeson.
            WHERE d.start_date::date >= CURRENT_DATE
              AND d.start_date::date <= CURRENT_DATE + INTERVAL '24 months'
              AND d.price_dkk IS NOT NULL
            GROUP BY d.operator, d.tour_slug, {season_case}
        )
        SELECT
            a.operator,
            a.tour_slug,
            a.tour_name,
            a.url,
            a.duration_days,
            a.from_price_dkk,
            a.tour_format,
            ss.season,
            ss.afgange,
            ss.min_pris,
            ss.max_pris,
            COALESCE(ss.udsolgt, 0) AS udsolgt,
            COALESCE(ss.faa, 0) AS faa,
            COALESCE(ss.garanteret, 0) AS garanteret,
            COALESCE(ss.aaben, 0) AS aaben
        FROM all_tours a
        LEFT JOIN season_stats ss ON ss.operator = a.operator
                                 AND COALESCE(ss.tour_slug, '') = COALESCE(a.tour_slug, '')
        ORDER BY a.operator, a.tour_name, ss.season
    """, (*aliases, *aliases)).fetchall()
    conn.close()

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


# Eksplicit maaned-mapping for Topas's 7 individuelle rejseforslag, bekraeftet
# via deres "Hvornaar er det bedst at rejse"-sektion paa topas.dk. Bruges til
# at placere ture uden faste afgange i de rigtige saeson-tabeller.
_TOPAS_INDIVIDUEL_MAANEDER: dict[str, set[int]] = {
    # Diskoøen, isbjerge og Eqi Gletsjer  →  jun-aug ("sommermaanederne juni, juli og august")
    "diskooeen-isbjerge-og-eqi-gletsjer": {6, 7, 8},
    # Efterår og nordlys                  →  sep-nov ("fra september til november")
    "efteraar-og-nordlys-i-groenland":    {9, 10, 11},
    # Forår og midnatssol                 →  maj-jun ("forsommeren i maj og juni")
    "foraar-og-midnatssol-i-groenland":   {5, 6},
    # Fra Nuuk til Sydgrønland            →  jun-sep ("i perioden juni til september")
    "fra-nuuk-til-sydgroenland":          {6, 7, 8, 9},
    # Hundeslæde, Iglo Lodge              →  jan-mar ("perioden fra januar til marts")
    "hundeslaede-iglo-lodge-og-nordlys-i-ilulissat": {1, 2, 3},
    # Unik rundrejse i Diskobugten        →  jun-sep ("sommermaanederne fra juni til september")
    "unik-rundrejse-i-diskobugten":       {6, 7, 8, 9},
    # Nuuk og Ilulissat med kystskib      →  maj-dec ("fra maj til december")
    "nuuk-og-ilulissat-med-kystskib":     {5, 6, 7, 8, 9, 10, 11, 12},
}


def _months_for_url(url: str) -> set[int] | None:
    """Kortlagte rejse-maaneder for et rejseforslag (None hvis ukendt URL)."""
    if not url:
        return None
    for slug, months in _TOPAS_INDIVIDUEL_MAANEDER.items():
        if slug in url:
            return months
    return None


def _fmt_dkk(v) -> str:
    if pd.isna(v) or v is None:
        return "—"
    try:
        return f"{int(v):,}".replace(",", ".")
    except (ValueError, TypeError):
        return "—"


def _fmt_price_range(min_p, max_p) -> str:
    if pd.isna(min_p) or min_p is None:
        return "—"
    if pd.isna(max_p) or max_p is None or min_p == max_p:
        return _fmt_dkk(min_p)
    return f"{_fmt_dkk(min_p)} – {_fmt_dkk(max_p)}"


def _status_mix(row) -> str:
    parts = []
    if row["udsolgt"]:
        parts.append(f"🔴 {int(row['udsolgt'])}")
    if row["faa"]:
        parts.append(f"🟡 {int(row['faa'])}")
    if row["garanteret"]:
        parts.append(f"✅ {int(row['garanteret'])}")
    if row["aaben"]:
        parts.append(f"⚪ {int(row['aaben'])}")
    return " · ".join(parts) if parts else "—"


def _topas_first(operators: list[str]) -> list[str]:
    return (["Topas"] if "Topas" in operators else []) + sorted(
        o for o in operators if o != "Topas"
    )


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.markdown("# 🌍 Destination-overblik")

destinations = _load_destinations()
if not destinations:
    st.warning("Ingen destinationer i topas_catalog.")
    st.stop()

dest = st.selectbox(
    "Destination",
    options=[d[0] for d in destinations],
    format_func=lambda land: f"{land} ({dict(destinations)[land]} Topas-ture)",
    help="Lande hvor Topas har ture (fra topas_catalog), sorteret efter antal ture.",
)

st.caption(
    f"Konkurrence-positionering for Topas's ture i {dest} — sæson for sæson. "
    "🔴 Udsolgt · 🟡 Få pladser · ✅ Garanteret · ⚪ Åben — tal viser antal afgange."
)

df = _load_destination_data(dest)

if df.empty:
    st.warning(
        f"Ingen ture fundet for {dest}. Kør Discovery (🔭-fanen) for at "
        "finde konkurrent-ture, og godkend dem i Review-kandidater."
    )
    st.stop()

# --- Split i tur-metadata, sæson-rækker og "uden afgange" ---
meta_cols = ["operator", "tour_slug", "tour_name", "url",
             "duration_days", "from_price_dkk", "tour_format"]
tours_meta = df[meta_cols].drop_duplicates(subset=["operator", "tour_name", "url"]).copy()

season_rows = df[df["season"].notna()].copy()
season_rows["er_forslag"] = False

# Rejseforslag uden faste afgange men med kortlagt rejseperiode → syntetiske
# sæson-rækker (vises med fra-pris, uden afgangs-tal).
_has_deps = set(zip(season_rows["operator"], season_rows["url"]))
_synth: list[dict] = []
for _, _r in tours_meta.iterrows():
    if (_r["operator"], _r["url"]) in _has_deps:
        continue
    _months = _months_for_url(_r["url"])
    if not _months:
        continue
    for _skey in {_SEASON_KEY[_month_to_season(m)] for m in _months if _month_to_season(m)}:
        _synth.append({
            **_r.to_dict(),
            "season": _skey,
            "afgange": None,
            "min_pris": _r["from_price_dkk"],
            "max_pris": None,
            "udsolgt": 0, "faa": 0, "garanteret": 0, "aaben": 0,
            "er_forslag": True,
        })
if _synth:
    season_rows = pd.concat([season_rows, pd.DataFrame(_synth)], ignore_index=True)

# Ture uden kommende afgange OG uden kortlagt periode → egen sektion nederst.
_covered = set(zip(season_rows["operator"], season_rows["url"]))
no_deps = tours_meta[
    ~tours_meta.apply(lambda r: (r["operator"], r["url"]) in _covered, axis=1)
].copy()

# --- Filter-bar ---
col_season, col_format, col_op, col_search = st.columns([1.5, 1.5, 1.5, 1.5])
with col_season:
    selected_seasons = st.multiselect(
        "Sæson",
        options=SEASONS,
        default=SEASONS,
        format_func=lambda s: SEASON_LABELS[s],
    )
with col_format:
    # Gorm vil se BAADE faellesrejser og individuelle som default.
    all_formats = sorted(df["tour_format"].dropna().unique().tolist())
    selected_formats = st.multiselect(
        "Type",
        options=all_formats,
        default=all_formats,
        help="Default viser alle typer. Fravælg for at fokusere "
             "sammenligningen (fx kun Fællesrejse mod Topas).",
    )
with col_op:
    selected_ops = st.multiselect(
        "Operator",
        options=_topas_first(df["operator"].dropna().unique().tolist()),
        default=_topas_first(df["operator"].dropna().unique().tolist()),
    )
with col_search:
    search = st.text_input("🔎 Søg i tur-navn", placeholder="fx Ilulissat")


def _apply_filters(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame[
        frame["operator"].isin(selected_ops)
        & frame["tour_format"].fillna("").isin(selected_formats)
    ]
    if search:
        out = out[out["tour_name"].fillna("").str.contains(search, case=False, na=False)]
    return out


fr = _apply_filters(season_rows)
no_deps_f = _apply_filters(no_deps)
selected_keys = {_SEASON_KEY[s] for s in selected_seasons}
fr_selected = fr[fr["season"].isin(selected_keys)]

# Hvis kun Topas i resultater, vis hint om konkurrent-data
if fr_selected["operator"].nunique() == 1 and "Topas" in fr_selected["operator"].values:
    st.info(
        f"💡 **Kun Topas-ture i resultat.** Konkurrent-data mangler for {dest}. "
        "Kør Discovery (🔭-fanen) på relevante konkurrenter → "
        "godkend deres ture som leads i Review-kandidater."
    )

st.divider()

# --- KPI-bar (kun valgte sæsoner; hver afgang tælles præcis én gang) ---
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Operatorer", fr_selected["operator"].nunique())
m2.metric("Ture", fr_selected.drop_duplicates(subset=["operator", "tour_name"]).shape[0])
_afg_total = int(fr_selected["afgange"].fillna(0).sum())
m3.metric("Afgange (24 mdr)", _afg_total)
_priced = fr_selected[fr_selected["min_pris"].notna()]
if not _priced.empty:
    _max_side = _priced["max_pris"].fillna(_priced["min_pris"])
    m4.metric(
        "Pris-spænd (kr.)",
        f"{int(_priced['min_pris'].min() / 1000)}k – {int(_max_side.max() / 1000)}k",
    )
else:
    m4.metric("Pris-spænd", "—")
_udsolgt = int(fr_selected["udsolgt"].sum())
m5.metric(
    "% Udsolgt",
    f"{(_udsolgt / _afg_total * 100):.0f}%" if _afg_total else "—",
    help=f"{_udsolgt} udsolgte afgange ud af {_afg_total} i de valgte sæsoner",
)


def _highlight_topas(row):
    if row["Operator"] == "Topas":
        return ["background-color: #fef3c7; font-weight: 600"] * len(row)
    return [""] * len(row)


def _render_season(season: str) -> None:
    skey = _SEASON_KEY[season]
    rows = fr[fr["season"] == skey]
    if rows.empty:
        return

    afg = int(rows["afgange"].fillna(0).sum())
    st.markdown(f"## {SEASON_LABELS[season]}")
    st.caption(
        f"{rows.drop_duplicates(subset=['operator', 'tour_name']).shape[0]} ture "
        f"fra {rows['operator'].nunique()} operatør(er) · {afg} afgange i sæsonen "
        "(næste 24 mdr)"
    )

    # --- Operator-sammendrag: hvordan ligger vi mod konkurrenterne HER ---
    grp = rows.groupby("operator").agg(
        Ture=("tour_name", "nunique"),
        Afgange=("afgange", lambda s: int(s.fillna(0).sum())),
        _billigste=("min_pris", "min"),
        _median=("min_pris", "median"),
        _udsolgt=("udsolgt", "sum"),
    ).reset_index().rename(columns={"operator": "Operator"})
    grp["Billigste"] = grp["_billigste"].apply(_fmt_dkk)
    grp["Median-pris"] = grp["_median"].apply(_fmt_dkk)
    grp["% udsolgt"] = grp.apply(
        lambda r: f"{(r['_udsolgt'] / r['Afgange'] * 100):.0f}%" if r["Afgange"] else "—",
        axis=1,
    )
    grp["_is_topas"] = grp["Operator"] == "Topas"
    grp = grp.sort_values(["_is_topas", "Afgange"], ascending=[False, False])
    grp = grp[["Operator", "Ture", "Afgange", "Billigste", "Median-pris", "% udsolgt"]]
    st.dataframe(
        grp.style.apply(_highlight_topas, axis=1),
        width="stretch",
        hide_index=True,
        column_config={
            "Ture": st.column_config.NumberColumn(width="small", format="%d"),
            "Afgange": st.column_config.NumberColumn(width="small", format="%d"),
        },
    )

    # --- Per-tur detaljer (sæson-scopede tal) ---
    display_df = rows.assign(
        is_topas=rows["operator"] == "Topas",
        Pris=rows.apply(
            lambda r: (
                f"{_fmt_dkk(r['min_pris'])} (fra-pris)" if r["er_forslag"]
                else _fmt_price_range(r["min_pris"], r["max_pris"])
            ),
            axis=1,
        ),
        Status=rows.apply(
            lambda r: "✈ rejseforslag" if r["er_forslag"] else _status_mix(r),
            axis=1,
        ),
    ).sort_values(
        ["is_topas", "afgange", "min_pris"],
        ascending=[False, False, True],
        na_position="last",
    )

    cols = ["operator", "tour_name", "tour_format", "duration_days",
            "afgange", "Pris", "Status", "url"]
    display_df = display_df[cols].rename(columns={
        "operator": "Operator",
        "tour_name": "Tur",
        "tour_format": "Format",
        "duration_days": "Dage",
        "afgange": "Afgange",
        "url": "Link",
    })

    with st.expander(f"Alle {len(display_df)} ture i {SEASON_LABELS[season]}", expanded=True):
        st.dataframe(
            display_df.style.apply(_highlight_topas, axis=1),
            width="stretch",
            hide_index=True,
            column_config={
                "Link": st.column_config.LinkColumn(
                    "Link", display_text="🔗", width="small",
                ),
                "Afgange": st.column_config.NumberColumn(width="small", format="%d"),
                "Dage": st.column_config.NumberColumn(width="small", format="%d"),
                "Format": st.column_config.TextColumn(width="small"),
            },
        )


for season in SEASONS:
    if season in selected_seasons:
        _render_season(season)

# --- Ture uden offentliggjorte afgange ---
if not no_deps_f.empty:
    with st.expander(f"⏳ Uden offentliggjorte afgange ({len(no_deps_f)})"):
        st.caption(
            "Typisk ture hvor næste sæsons datoer ikke er lagt på sitet endnu "
            "(fx sommerture efter sæsonafslutning). Fra-prisen er seneste kendte."
        )
        nd = no_deps_f.assign(
            **{"Fra-pris": no_deps_f["from_price_dkk"].apply(_fmt_dkk)}
        )[["operator", "tour_name", "tour_format", "duration_days", "Fra-pris", "url"]]
        nd = nd.rename(columns={
            "operator": "Operator", "tour_name": "Tur",
            "tour_format": "Format", "duration_days": "Dage", "url": "Link",
        }).sort_values(["Operator", "Tur"])
        st.dataframe(
            nd.style.apply(_highlight_topas, axis=1),
            width="stretch",
            hide_index=True,
            column_config={
                "Link": st.column_config.LinkColumn("Link", display_text="🔗", width="small"),
                "Dage": st.column_config.NumberColumn(width="small", format="%d"),
            },
        )


with st.expander("ℹ Hvordan virker denne side?"):
    st.markdown(
        """
**Datakilde:** `tours`-tabellen + `departures` + `topas_catalog`.

**Destination-match:** en tur hører til destinationen hvis dens scrapede
`country` matcher — eller, hvis feltet er tomt, hvis den er kortlagt til en
Topas-tur (`competes_with`) i det valgte land.

**Sæson-tal er sæson-scopede:** hver sæson-tabel viser KUN de afgange og
priser der ligger i den sæson (næste 24 måneder). En tur med afgange i både
juni og december optræder i begge tabeller — med hver sin delmængde.
- 🌞 Højsæson: jun-aug (6, 7, 8)
- ❄️ Mellemsæson: feb-maj + sep (2, 3, 4, 5, 9)
- 🌑 Lavsæson: okt-jan (1, 10, 11, 12)

**Operator-sammendraget** øverst i hver sæson viser hvordan operatørerne
ligger mod hinanden: antal ture og afgange, billigste pris, median-pris og
udsolgt-procent — kun for den sæson.

**Rejseforslag** (Topas's individuelle ture uden faste afgange) placeres i de
sæsoner topas.dk selv anbefaler, vises med fra-pris og markeres "✈ rejseforslag".

**Uden offentliggjorte afgange:** ture hvor der ikke ligger kommende datoer på
operatørens site endnu (fx sommerture lige efter sæsonen) samles nederst i
stedet for at stå med 0 og — i sæson-tabellerne.

**Fra-pris fallback:** hvis scraperen ikke fik fra-prisen med, bruges Topas-
katalogets pris for Topas-ture.

**Klassifikationer** (Fællesrejse/Rejs på egen hånd/Krydstogt) der er manuelt
verificeret ligger i `tour_overrides` og overlever re-scrapes.

**Mangler konkurrent-data for en destination?** Kør Discovery (🔭-fanen),
godkend leads i Review-kandidater (pages/5) — de kommer med i næste scrape.
"""
    )
