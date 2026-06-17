"""
Grønland-overblik — konkurrence-positionering for Topas's Grønlandsture.

Hovedformaal: hvordan ligger Topas ift. konkurrenterne paa Groenland?
- Antal afgange, pris-spaend, status-fordeling pr. operator pr. saeson
- Topas's 13 Groenlandsture i context af konkurrent-udbud

Saeson-bestemmelse pr. tur via dens departure-maaneder:
- Hoejsaeson: jun-sep (6,7,8,9)
- Mellemsaeson: feb-maj (2,3,4,5)
- Lavsaeson: okt-jan (1,10,11,12)
En tur kan tilhoere flere saesoner.

Datakilde: tours + departures + topas_catalog (for Topas-ture uden tours-row).
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Grønland · Topas", page_icon="🏔️", layout="wide")

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
    SEASON_HIGH: "Højsæson (jun-sep)",
    SEASON_MID: "Mellemsæson (feb-maj)",
    SEASON_LOW: "Lavsæson (okt-jan)",
}
SEASON_MONTHS = {
    SEASON_HIGH: {6, 7, 8, 9},
    SEASON_MID: {2, 3, 4, 5},
    SEASON_LOW: {1, 10, 11, 12},
}


def _month_to_season(m: int) -> str:
    for s, months in SEASON_MONTHS.items():
        if m in months:
            return s
    return ""


# ---------------------------------------------------------------------------
# Data-loading
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600)
def _load_greenland_tours() -> pd.DataFrame:
    """Hent alle Groenlandsture med departures-aggregater.

    Returnerer én row pr. (operator, tour) med:
    - tour_name, tour_format, duration_days, url
    - antal_afgange (naeste 12 mdr)
    - min_pris, max_pris (DKK)
    - udsolgt/faa/garanteret/aaben counts
    - maaneder (kommasepareret liste af unique departure-maaneder)
    """
    from topas_scraper._pg_conn import connect as pg_connect  # noqa: PLC0415

    conn = pg_connect()
    rows = conn.execute("""
        WITH greenland_tours AS (
            SELECT DISTINCT
                t.operator,
                t.tour_slug,
                t.tour_name,
                t.url,
                t.duration_days,
                t.from_price_dkk,
                t.tour_format
            FROM tours t
            -- Eksakt match paa kendte country-vaerdier (ikke wildcard-LIKE
            -- som ogsaa fangede "Graekenland" pga greediness).
            -- NB: literale procent-tegn escapes som dobbelt-procent fordi
            -- psycopg2 ellers parser dem som parameter-placeholders.
            WHERE LOWER(t.country) IN ('grønland', 'groenland', 'greenland')
               -- Fallback: tours hvor scraper ikke extrahede 'Grønland' som
               -- country (typisk Greenland-specialister), men competes_with
               -- peger paa en Topas GL-kode (GLBL/GLFS/.../GLXS).
               -- NB: begraenset til country tom eller alm. Groenland for
               -- at undgaa falsk match paa fx Topas-koder der ogsaa starter
               -- med GL men ikke er Groenlands-ture.
               OR (t.competes_with IS NOT NULL
                   AND t.competes_with LIKE 'GL%%'
                   AND COALESCE(t.country, '') IN ('', 'Grønland', 'Greenland', 'Groenland'))
        ),
        topas_extra AS (
            -- Topas-ture i topas_catalog som ikke har tours-row endnu
            SELECT
                'Topas' AS operator,
                NULL AS tour_slug,
                tc.tour_name,
                tc.url,
                tc.duration_days,
                tc.from_price_dkk,
                'Fællesrejse' AS tour_format
            FROM topas_catalog tc
            WHERE tc.country = 'Grønland'
              AND (tc.audience_segment IS NULL OR tc.audience_segment != 'Udgået')
              AND NOT EXISTS (
                  SELECT 1 FROM tours t
                  WHERE t.operator = 'Topas' AND t.url = tc.url
              )
        ),
        all_tours AS (
            SELECT * FROM greenland_tours
            UNION ALL
            SELECT * FROM topas_extra
        ),
        dep_stats AS (
            SELECT
                d.operator,
                d.tour_slug,
                COUNT(*) AS antal_afgange,
                MIN(d.price_dkk)::int AS min_pris,
                MAX(d.price_dkk)::int AS max_pris,
                SUM(CASE WHEN d.availability_status = 'Udsolgt' THEN 1 ELSE 0 END) AS udsolgt,
                SUM(CASE WHEN d.availability_status = 'Få pladser' THEN 1 ELSE 0 END) AS faa,
                SUM(CASE WHEN d.availability_status = 'Garanteret' THEN 1 ELSE 0 END) AS garanteret,
                SUM(CASE WHEN d.availability_status = 'Åben' THEN 1 ELSE 0 END) AS aaben,
                STRING_AGG(DISTINCT EXTRACT(MONTH FROM d.start_date::date)::int::text, ',') AS maaneder
            FROM departures d
            WHERE d.start_date::date >= CURRENT_DATE
              AND d.start_date::date <= CURRENT_DATE + INTERVAL '12 months'
              AND d.price_dkk IS NOT NULL
            GROUP BY d.operator, d.tour_slug
        )
        SELECT
            a.operator,
            a.tour_slug,
            a.tour_name,
            a.url,
            a.duration_days,
            a.tour_format,
            COALESCE(ds.min_pris, a.from_price_dkk) AS min_pris,
            COALESCE(ds.max_pris, a.from_price_dkk) AS max_pris,
            COALESCE(ds.antal_afgange, 0) AS antal_afgange,
            COALESCE(ds.udsolgt, 0) AS udsolgt,
            COALESCE(ds.faa, 0) AS faa,
            COALESCE(ds.garanteret, 0) AS garanteret,
            COALESCE(ds.aaben, 0) AS aaben,
            COALESCE(ds.maaneder, '') AS maaneder
        FROM all_tours a
        LEFT JOIN dep_stats ds ON ds.operator = a.operator
                              AND COALESCE(ds.tour_slug, '') = COALESCE(a.tour_slug, '')
        ORDER BY a.operator, a.tour_name
    """).fetchall()

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


def _tour_seasons(months_str: str) -> set[str]:
    if not months_str:
        return set()
    try:
        months = {int(m) for m in months_str.split(",") if m}
    except ValueError:
        return set()
    out = {_month_to_season(m) for m in months}
    out.discard("")
    return out


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


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.markdown("# 🏔️ Grønland-overblik")
st.caption(
    "Konkurrence-positionering for Topas's Grønlandsture. "
    "🔴 Udsolgt · 🟡 Få pladser · ✅ Garanteret · ⚪ Åben — tal viser antal afgange."
)

df = _load_greenland_tours()

if df.empty:
    st.warning("Ingen Grønlandsture i databasen.")
    st.stop()

# Beregn saesoner pr. tur
df["seasons_set"] = df["maaneder"].apply(_tour_seasons)

# Filter-bar
col_season, col_format, col_op, col_search = st.columns([1.5, 1.5, 1.5, 1.5])
with col_season:
    selected_seasons = st.multiselect(
        "Sæson",
        options=SEASONS,
        default=SEASONS,
        format_func=lambda s: SEASON_LABELS[s],
    )
with col_format:
    # Tour-format filter — Gorm vil se BAADE faellesrejser og individuelle som
    # default (jf. user-feedback: "Vi vil baade ha informationen omkring
    # gruppe rejser samt individuelle rejser"). Filter er stadig der hvis man
    # vil snaevre ned til aebler/aebler-sammenligning med Topas's faellesrejser.
    all_formats = sorted(df["tour_format"].dropna().unique().tolist())
    selected_formats = st.multiselect(
        "Type",
        options=all_formats,
        default=all_formats,
        help="Default viser alle typer. Topas laver kun Fællesrejser; "
             "konkurrenter har også 'Rejs på egen hånd' og krydstogter. "
             "Fravælg en type for at fokusere sammenligningen.",
    )
with col_op:
    all_operators = sorted(df["operator"].dropna().unique().tolist())
    if "Topas" in all_operators:
        all_operators = ["Topas"] + [o for o in all_operators if o != "Topas"]
    selected_ops = st.multiselect(
        "Operator", options=all_operators, default=all_operators
    )
with col_search:
    search = st.text_input("🔎 Søg i tur-navn", placeholder="fx Ilulissat")

# Apply filters
selected_seasons_set = set(selected_seasons)
filtered = df[
    df["operator"].isin(selected_ops)
    & df["tour_format"].fillna("").isin(selected_formats)
    & df["seasons_set"].apply(lambda s: bool(s & selected_seasons_set))
].copy()

# Søg
if search:
    mask = filtered["tour_name"].fillna("").str.contains(search, case=False, na=False)
    filtered = filtered[mask]

# Hvis kun Topas i resultater, vis hint om konkurrent-data
non_topas_count = len(filtered[filtered["operator"] != "Topas"])
topas_count = len(filtered[filtered["operator"] == "Topas"])
if non_topas_count == 0 and topas_count > 0:
    st.info(
        "💡 **Kun Topas-ture i resultat.** Konkurrent-data mangler for Grønland. "
        "Kør Discovery (🔭-fanen) på Greenland Travel, Albatros m.fl. → "
        "godkend Grønlands-ture som leads i Review-kandidater. "
        "Når approved bliver de scrapet ugentligt og dukker op her."
    )

st.divider()

# KPI-bar
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Operatorer", len(filtered["operator"].unique()))
m2.metric("Ture", len(filtered))
m3.metric("Afgange (12 mdr)", int(filtered["antal_afgange"].sum()))
all_prices = filtered[filtered["min_pris"].notna()]
if not all_prices.empty:
    m4.metric(
        "Pris-spænd (kr.)",
        f"{int(all_prices['min_pris'].min() / 1000)}k – {int(all_prices['max_pris'].max() / 1000)}k",
    )
else:
    m4.metric("Pris-spænd", "—")
udsolgt = int(filtered["udsolgt"].sum())
afg_total = int(filtered["antal_afgange"].sum())
m5.metric(
    "% Udsolgt",
    f"{(udsolgt / afg_total * 100):.0f}%" if afg_total else "—",
    help=f"{udsolgt} udsolgte afgange ud af {afg_total}",
)

# Pr.-sæson sektioner
def _render_season(season: str, tours: pd.DataFrame) -> None:
    in_season = tours[tours["seasons_set"].apply(lambda s: season in s)]
    if in_season.empty:
        return

    op_count = len(in_season["operator"].unique())
    tour_count = len(in_season)
    afg = int(in_season["antal_afgange"].sum())

    st.markdown(f"## {SEASON_LABELS[season]}")
    st.caption(
        f"{tour_count} ture fra {op_count} operatør(er) · {afg} afgange næste 12 mdr"
    )

    display_df = in_season.assign(
        is_topas=in_season["operator"] == "Topas",
        Pris=in_season.apply(lambda r: _fmt_price_range(r["min_pris"], r["max_pris"]), axis=1),
        Status=in_season.apply(_status_mix, axis=1),
    ).sort_values(
        ["is_topas", "antal_afgange", "min_pris"],
        ascending=[False, False, True],
    )

    cols = ["operator", "tour_name", "tour_format", "duration_days",
            "antal_afgange", "Pris", "Status", "url"]
    display_df = display_df[cols].rename(columns={
        "operator": "Operator",
        "tour_name": "Tur",
        "tour_format": "Format",
        "duration_days": "Dage",
        "antal_afgange": "Afgange",
        "url": "Link",
    })

    def _highlight_topas(row):
        if row["Operator"] == "Topas":
            return ["background-color: #fef3c7; font-weight: 600"] * len(row)
        return [""] * len(row)

    styled = display_df.style.apply(_highlight_topas, axis=1)
    st.dataframe(
        styled,
        use_container_width=True,
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
        _render_season(season, filtered)


with st.expander("ℹ Hvordan virker denne side?"):
    st.markdown(
        """
**Datakilde:** `tours`-tabellen + `departures` + `topas_catalog`.
Alle ture hvor `country` matcher Grønland/Groenland/Greenland.

**Sæson-bestemmelse pr. tur:** baseret på `start_date`-måneder for
departures i de næste 12 måneder.
- 🌞 Højsæson: jun-sep (6, 7, 8, 9)
- ❄️ Mellemsæson: feb-maj (2, 3, 4, 5)
- 🌑 Lavsæson: okt-jan (1, 10, 11, 12)

En tur kan tilhøre flere sæsoner (fx Trekking med afgange i både juni og juli).

**Topas-rows er fremhævet i gul** for visuelt at skille dem ud.

**Status-tal**: antal afgange i hver kategori (udsolgt/få pladser/garanteret/åben).
Pris-spænd viser min-max på tværs af alle afgange.

**Mangler konkurrent-data?** Kør Discovery på Greenland Travel, Albatros m.fl.,
godkend Grønland-relevante leads i Review-kandidater (pages/5). De scrapes så
ugentligt og dukker automatisk op her.
"""
    )
