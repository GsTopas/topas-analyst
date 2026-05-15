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


# === Kompakt header: titel + total + sync-info + maaneds-filter paa én linje ===
synced_at = df["synced_at"].max()
if pd.notna(synced_at):
    if hasattr(synced_at, "tz_convert"):
        synced_at = synced_at.tz_convert("Europe/Copenhagen")
    sync_label = synced_at.strftime("%d. %b kl. %H:%M")
else:
    sync_label = "ukendt"

grand_total = df["db_budget_diff"].sum()
grand_sign = "+" if grand_total >= 0 else "-"
grand_total_str = f"{grand_sign}{abs(int(round(grand_total))):,}".replace(",", ".")

# Liste af maaneder der HAR data
available_months_num = sorted(df["month_num"].dropna().unique().tolist())
available_months_names = [MONTH_ORDER[m - 1] for m in available_months_num]

col_title, col_total, col_months = st.columns([1.3, 1, 3])
with col_title:
    st.markdown(
        f"### 📈 Forecasting  \n"
        f"<span style='color:#888;font-size:0.85em'>Synced {sync_label} · "
        f"{len(df)} rækker</span>",
        unsafe_allow_html=True,
    )
with col_total:
    st.markdown(
        f"<div style='text-align:right;'>"
        f"<span style='color:#888;font-size:0.85em'>Total ift. budget</span><br>"
        f"<span style='font-size:1.5em;font-weight:600'>{grand_total_str} kr.</span>"
        f"</div>",
        unsafe_allow_html=True,
    )
with col_months:
    selected_month_names = st.multiselect(
        "Måneder",
        options=available_months_names,
        default=available_months_names,
        label_visibility="visible",
    )

# Apply filter
selected_month_nums = [MONTH_ORDER.index(n) + 1 for n in selected_month_names]
df_filt = df[df["month_num"].isin(selected_month_nums)].copy()

months_with_data = sorted(df_filt["month_num"].dropna().unique().tolist())

if not months_with_data:
    st.info("Vælg mindst én måned.")
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

# Ikon-prefix saa man straks ser at det er en underrubrik (ikke en turkode).
CATEGORY_ICONS = {
    "TOPAS": "▸ TOPAS",
    "GREENLAND BY TOPAS": "▸ GREENLAND BY TOPAS",
    "VIETNAM BY TOPAS": "▸ VIETNAM BY TOPAS",
}


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

        rows.append((CATEGORY_ICONS[cat_name], ""))  # kategori-header med ikon-prefix
        for _, r in sub.iterrows():
            rows.append((r["tour_code"], _fmt_kr(r["db_budget_diff"])))

        cat_total = sub["db_budget_diff"].sum()
        rows.append((f"   {cat_name} total", _fmt_kr(cat_total)))  # indent saa sub-total er visuelt under kategori
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

# === Styling ===
CATEGORY_HEADER_VALUES = set(CATEGORY_ICONS.values())
SUBTOTAL_SUFFIX = " total"

def _row_style(row: pd.Series) -> list[str]:
    """Returnerer CSS pr. celle for én raekke."""
    tour_cell = ""
    for (_header, sub), val in row.items():
        if sub == "Tur":
            tour_cell = str(val)
            break

    base_styles: list[str] = []
    tour_stripped = tour_cell.strip()
    is_cat_header = tour_cell in CATEGORY_HEADER_VALUES
    is_subtotal = tour_stripped.endswith(SUBTOTAL_SUFFIX) and tour_stripped != "Total"
    is_grand_total = tour_stripped == "Total"

    for (_header, sub), val in row.items():
        style = ""
        if is_cat_header:
            # Tydelig sub-rubrik: kraftig blaa baggrund + hvid tekst + uppercase
            style = (
                "background-color:#1e3a5f; "
                "color:#ffffff; "
                "font-weight:700; "
                "font-size:0.95rem; "
                "letter-spacing:0.5px; "
                "padding:8px 4px;"
            )
        elif is_subtotal:
            style = (
                "font-weight:600; "
                "font-style:italic; "
                "border-top:1px dashed #94a3b8; "
                "color:#475569; "
                "background-color:#f8fafc;"
            )
        elif is_grand_total:
            style = (
                "background-color:#fff4e6; "
                "color:#7c2d12; "
                "font-weight:800; "
                "font-size:1.05rem; "
                "border-top:2px solid #d97706;"
            )
        else:
            # Almindelige data-celler: farve negative tal roede, positive groenne
            if sub == "DB budget forskel" and isinstance(val, str) and val:
                if val.startswith("-"):
                    style = "color:#c0392b; font-weight:500;"
                elif val and val != "0":
                    style = "color:#1e8449; font-weight:500;"
        base_styles.append(style)
    return base_styles


styled = table.style.apply(_row_style, axis=1)

# Custom CSS for tabellens headers (MultiIndex). Streamlit's stDataFrame
# er Arrow-baseret, men vi kan style headers via inline CSS injection.
st.markdown(
    """
    <style>
    /* Maaneds-headers (top level) */
    [data-testid="stDataFrame"] thead tr:first-child th {
        font-weight: 700 !important;
        font-size: 0.95rem !important;
        background-color: #f8fafc !important;
        color: #0f172a !important;
        border-bottom: 1px solid #cbd5e1 !important;
    }
    /* Sub-headers (Tur / DB budget forskel) */
    [data-testid="stDataFrame"] thead tr:nth-child(2) th {
        font-weight: 600 !important;
        color: #475569 !important;
    }
    /* Data-rows */
    [data-testid="stDataFrame"] tbody td {
        font-size: 0.9rem !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Dybere default-hoejde saa hele tabellen er synlig uden scroll i de fleste maaneder.
_row_h = 35
_header_h = 80
_target_h = min(900, _header_h + _row_h * (len(table) + 1))
st.dataframe(styled, use_container_width=True, hide_index=True, height=_target_h)


with st.expander("ℹ Hvor kommer data fra?"):
    st.markdown(
        "Data hentes dagligt fra `K:\\OFFICE\\Operations\\Turregnskab\\Opfølgning\\"
        "Turomkostninger 2026.xls` af scriptet `scripts/sync_forecast.ps1` "
        "der kører via Windows Task Scheduler. Filen åbnes read-only via "
        "Excel COM-automation — ingen ændringer skrives tilbage.\n\n"
        "**Filter-regel:** kun rækker hvor kolonne B (Turkode), I (Realiseret DB) og "
        "M (DB-forskel) alle har værdi, regnes som realiserede ture og indlæses."
    )
