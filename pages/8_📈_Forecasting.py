"""
Forecasting — DB-budget-forskel pr. tur pr. måned.

Læser data fra Supabase-tabellen `tour_pl_forecast` der populeres dagligt
af scripts/sync_forecast.ps1 fra K:\\OFFICE\\Operations\\Turregnskab\\
Opfølgning\\Turomkostninger 2026.xls.

Vis: matrix med turkode som række, måned som kolonne, DB-forskel som celle.
Plus pr. måneds-total og grand-total.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Forecasting · Topas", page_icon="📈", layout="wide")

# Password-gate
from topas_scraper._auth import require_auth  # noqa: E402
require_auth()


st.markdown("# 📈 Forecasting")
st.caption(
    "DB-budget-forskel pr. tur pr. måned, synced fra Turomkostninger 2026.xls. "
    "Kun realiserede ture (rækker hvor 'Realiseret DB' er udfyldt) vises."
)


MONTH_ORDER = [
    "Januar", "Februar", "Marts", "April", "Maj", "Juni",
    "Juli", "August", "September", "Oktober", "November", "December",
]


@st.cache_data(ttl=600)
def _load_forecast() -> pd.DataFrame:
    """Hent forecast-data fra Supabase. Cached 10 min."""
    from topas_scraper._pg_conn import connect as pg_connect  # noqa: PLC0415

    conn = pg_connect()
    rows = conn.execute("""
        SELECT month, month_num, tour_code, homecoming_date,
               budget_db, realiseret_db, db_budget_diff,
               pax_diff, dg_diff, synced_at
        FROM tour_pl_forecast
        ORDER BY month_num, homecoming_date, tour_code
    """).fetchall()

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


df = _load_forecast()

if df.empty:
    st.warning(
        "Ingen forecast-data i Supabase endnu. Kør `scripts/sync_forecast.ps1` "
        "lokalt for at populere tabellen."
    )
    st.stop()


# Header med sync-info
synced_at = df["synced_at"].max()
if pd.notna(synced_at):
    if hasattr(synced_at, "tz_convert"):
        synced_at = synced_at.tz_convert("Europe/Copenhagen")
    sync_label = synced_at.strftime("%d. %b %Y kl. %H:%M")
else:
    sync_label = "ukendt"

col_meta, col_total = st.columns([3, 2])
with col_meta:
    st.caption(f"Senest synced: **{sync_label}** · {len(df)} rækker · "
               f"{df['tour_code'].nunique()} unikke turkoder")
with col_total:
    grand_total = df["db_budget_diff"].sum()
    st.metric("Total DB-forskel ift. budget", f"{grand_total:,.0f} kr.".replace(",", "."))

st.divider()


# === Filter-bar ===
col1, _ = st.columns([1, 3])
with col1:
    tour_prefix = st.text_input(
        "Filtrer turkoder (prefix)",
        value="",
        placeholder="fx ESMV, NOSS, IVXX ...",
        help="Vis kun turkoder der starter med denne tekst. Tom = alle."
    )

# Apply filter
df_filt = df.copy()
if tour_prefix.strip():
    df_filt = df_filt[df_filt["tour_code"].str.startswith(tour_prefix.strip().upper(), na=False)]


# === Side-om-side maaned-kolonner ===
# For hver maaned: én kolonne med turkoder + én med DB-budget forskel.
# Maaneder har forskelligt antal raekker → pad med tomme strenge saa de
# kan flettes i én tabel.

months_with_data = sorted(
    df_filt["month_num"].dropna().unique().tolist()
)

if not months_with_data:
    st.info("Ingen rækker matcher dit filter.")
    st.stop()

def _fmt_kr(v) -> str:
    if pd.isna(v) or v is None or v == "":
        return ""
    try:
        return f"{int(round(float(v))):,}".replace(",", ".")
    except (ValueError, TypeError):
        return ""


SPECIAL_CODES = {"Oplæring", "Research"}

def _categorize(code: str) -> str:
    """Tilskriv turkode til en af de tre kategorier."""
    if code in SPECIAL_CODES:
        return "TOPAS"
    if code.startswith("IG"):
        return "GREENLAND BY TOPAS"
    if code.startswith("IV"):
        return "VIETNAM BY TOPAS"
    return "TOPAS"


CATEGORY_ORDER = ["TOPAS", "GREENLAND BY TOPAS", "VIETNAM BY TOPAS"]


def _build_month_rows(g: pd.DataFrame) -> list[tuple[str, str]]:
    """Returnér liste af (tur-celle, diff-celle) for én måned med kategori-
    headere, sub-totaler og en blank linje mellem kategorier."""
    rows: list[tuple[str, str]] = []
    cats: dict[str, pd.DataFrame] = {
        c: g[g["tour_code"].apply(_categorize) == c]
        for c in CATEGORY_ORDER
    }
    for cat_name in CATEGORY_ORDER:
        sub = cats[cat_name]
        if sub.empty:
            continue

        # Sortér: dato-baserede ture først, Oplæring/Research nederst i TOPAS
        sub = sub.copy()
        sub["_special"] = sub["tour_code"].isin(SPECIAL_CODES)
        sub = sub.sort_values(
            ["_special", "homecoming_date", "tour_code"],
            na_position="last",
        )

        rows.append((cat_name, ""))  # kategori-header
        for _, r in sub.iterrows():
            rows.append((r["tour_code"], _fmt_kr(r["db_budget_diff"])))

        cat_total = sub["db_budget_diff"].sum()
        rows.append((f"{cat_name} total", _fmt_kr(cat_total)))
        rows.append(("", ""))  # spacer

    # Fjern sidste spacer hvis den er der
    while rows and rows[-1] == ("", ""):
        rows.pop()
    return rows


# Byg rows pr. måned + find længste
month_rows: dict[int, list[tuple[str, str]]] = {}
for m_num in months_with_data:
    g = df_filt[df_filt["month_num"] == m_num]
    month_rows[m_num] = _build_month_rows(g)

max_rows = max(len(r) for r in month_rows.values())

# Byg MultiIndex-kolonner: (maaned-med-total, Tur|DB budget forskel)
columns = []
data: dict[tuple, list] = {}
month_totals: dict[int, float] = {}
for m_num in months_with_data:
    month_name = MONTH_ORDER[m_num - 1]
    g = df_filt[df_filt["month_num"] == m_num]
    total = g["db_budget_diff"].sum()
    month_totals[m_num] = total

    sign = "+" if total >= 0 else "-"
    header = f"{month_name}  ·  {sign}{_fmt_kr(abs(total))} kr."

    # Tilfoej Oplaering+Research-diff i parentes (hvis der er data)
    opl_res = g[g["tour_code"].isin(SPECIAL_CODES)]["db_budget_diff"].sum()
    if not g[g["tour_code"].isin(SPECIAL_CODES)].empty:
        or_sign = "+" if opl_res >= 0 else "-"
        header += f"  (Opl/Res: {or_sign}{_fmt_kr(abs(opl_res))} kr.)"

    rows = month_rows[m_num]
    tour_col = [t for (t, _) in rows] + [""] * (max_rows - len(rows))
    diff_col = [d for (_, d) in rows] + [""] * (max_rows - len(rows))
    data[(header, "Tur")] = tour_col
    data[(header, "DB budget forskel")] = diff_col
    columns.append((header, "Tur"))
    columns.append((header, "DB budget forskel"))

table = pd.DataFrame(data, columns=pd.MultiIndex.from_tuples(columns))

# Grand-Total-raekke nederst
total_row: dict[tuple, str] = {}
for (header, sub) in columns:
    m_num = next(mn for mn in months_with_data if MONTH_ORDER[mn - 1] in header)
    if sub == "Tur":
        total_row[(header, sub)] = "Total"
    else:
        total_row[(header, sub)] = _fmt_kr(month_totals[m_num])

table.loc[len(table)] = pd.Series(total_row)

st.dataframe(table, use_container_width=True, hide_index=True)


with st.expander("ℹ Hvor kommer data fra?"):
    st.markdown(
        "Data hentes dagligt fra `K:\\OFFICE\\Operations\\Turregnskab\\Opfølgning\\"
        "Turomkostninger 2026.xls` af scriptet `scripts/sync_forecast.ps1` "
        "der kører via Windows Task Scheduler. Filen åbnes read-only via "
        "Excel COM-automation — ingen ændringer skrives tilbage.\n\n"
        "**Filter-regel:** kun rækker hvor kolonne B (Turkode), I (Realiseret DB) og "
        "M (DB-forskel) alle har værdi, regnes som realiserede ture og indlæses."
    )
