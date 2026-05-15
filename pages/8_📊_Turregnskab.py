"""
Turregnskab — DB-afvigelse ift. budget pr. tur pr. måned.

Læser data fra Supabase-tabellen `tour_pl_forecast` der populeres dagligt
af scripts/sync_forecast.ps1 fra K:\\OFFICE\\Operations\\Turregnskab\\
Opfølgning\\Turomkostninger 2026.xls.

Vis: matrix med turkode som række, måned som kolonne, DB-forskel som celle.
Plus pr. måneds-total og grand-total (YTD).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Turregnskab · Topas", page_icon="📊", layout="wide")

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
        f"### 📊 Turregnskab  \n"
        f"<span style='color:#888;font-size:0.85em'>Synced {sync_label} · "
        f"{len(df)} rækker</span>",
        unsafe_allow_html=True,
    )
with col_total:
    st.markdown(
        f"<div style='text-align:right;'>"
        f"<span style='color:#888;font-size:0.85em'>DB-afvigelse 2026 (YTD)</span><br>"
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


# Custom CSS for alle tabellers headers — fed paa tvers af de 3 faner.
st.markdown(
    """
    <style>
    [data-testid="stDataFrame"] thead tr:first-child th {
        font-weight: 800 !important;
        font-size: 0.95rem !important;
        background-color: #f8fafc !important;
        color: #0f172a !important;
        border-bottom: 1px solid #cbd5e1 !important;
    }
    [data-testid="stDataFrame"] thead tr:nth-child(2) th {
        font-weight: 700 !important;
        color: #1e3a5f !important;
        font-size: 0.9rem !important;
    }
    [data-testid="stDataFrame"] tbody td {
        font-size: 0.9rem !important;
    }
    /* Tab-labels selv */
    button[data-baseweb="tab"] p {
        font-weight: 700 !important;
        font-size: 0.95rem !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

CATEGORY_HEADER_VALUES = set(CATEGORY_ICONS.values())
SUBTOTAL_SUFFIX = " total"


def _detail_row_style(row: pd.Series) -> list[str]:
    """CSS pr. celle for detalje-tabellen."""
    tour_cell = ""
    for (_header, sub), val in row.items():
        if sub == "Tur":
            tour_cell = str(val)
            break

    styles: list[str] = []
    tour_stripped = tour_cell.strip()
    is_cat_header = tour_cell in CATEGORY_HEADER_VALUES
    is_subtotal = tour_stripped.endswith(SUBTOTAL_SUFFIX) and tour_stripped != "Total"
    is_grand_total = tour_stripped == "Total"

    for (_header, sub), val in row.items():
        s = ""
        if is_cat_header:
            s = ("background-color:#1e3a5f; color:#ffffff; font-weight:800; "
                 "font-size:0.95rem; letter-spacing:0.5px; padding:8px 4px;")
        elif is_subtotal:
            s = ("font-weight:700; font-style:italic; border-top:1px dashed #94a3b8; "
                 "color:#475569; background-color:#f8fafc;")
        elif is_grand_total:
            s = ("background-color:#fff4e6; color:#7c2d12; font-weight:800; "
                 "font-size:1.05rem; border-top:2px solid #d97706;")
        else:
            # Alle data-tal i fed
            if sub == "DB budget forskel" and isinstance(val, str) and val:
                if val.startswith("-"):
                    s = "color:#c0392b; font-weight:700;"
                elif val and val != "0":
                    s = "color:#1e8449; font-weight:700;"
                else:
                    s = "font-weight:700;"
            elif sub == "Tur" and val:
                # Turkode i semi-fed
                s = "font-weight:600;"
        styles.append(s)
    return styles


def _render_detail_view(df_in: pd.DataFrame, month_nums: list[int]) -> None:
    """Detalje-tabel: alle ture pr. maaned side om side, opdelt i kategorier
    (TOPAS / GREENLAND BY TOPAS / VIETNAM BY TOPAS) ligesom kilde-arket.

    NB: Maaneder har forskelligt antal ture i hver kategori, saa kategori-
    headers aligner ikke altid horisontalt paa tvers af kolonner. Vi viser
    dem alligevel fordi de matcher Excel-strukturen, og fed/farve-styling
    goer dem genkendelige selv naar de er forskudt."""
    month_rows: dict[int, list[tuple[str, str]]] = {}
    for m_num in month_nums:
        g = df_in[df_in["month_num"] == m_num]
        month_rows[m_num] = _build_month_rows(g)

    max_rows = max(len(r) for r in month_rows.values())

    columns = []
    data: dict[tuple, list] = {}
    month_totals: dict[int, float] = {}
    for m_num in month_nums:
        month_name = MONTH_ORDER[m_num - 1]
        g = df_in[df_in["month_num"] == m_num]
        total = g["db_budget_diff"].sum()
        month_totals[m_num] = total
        sign = "+" if total >= 0 else "-"
        header = f"{month_name}  ·  {sign}{_fmt_kr(abs(total))} kr."

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

    total_row: dict[tuple, str] = {}
    for (header, sub) in columns:
        m_num = next(mn for mn in month_nums if MONTH_ORDER[mn - 1] in header)
        if sub == "Tur":
            total_row[(header, sub)] = "Total"
        else:
            total_row[(header, sub)] = _fmt_kr(month_totals[m_num])
    table.loc[len(table)] = pd.Series(total_row)

    styled = table.style.apply(_detail_row_style, axis=1)
    _row_h, _header_h = 35, 80
    _target_h = min(900, _header_h + _row_h * (len(table) + 1))
    st.dataframe(styled, use_container_width=True, hide_index=True, height=_target_h)


def _render_summary_view(df_in: pd.DataFrame, month_nums: list[int]) -> None:
    """Maaneds-overblik: én række pr. maaned med Ture, Opl/Res og Total."""
    rows = []
    for m_num in month_nums:
        month_name = MONTH_ORDER[m_num - 1]
        g = df_in[df_in["month_num"] == m_num]
        ture = g[~g["tour_code"].isin(SPECIAL_CODES)]["db_budget_diff"].sum()
        opl_res = g[g["tour_code"].isin(SPECIAL_CODES)]["db_budget_diff"].sum()
        total = g["db_budget_diff"].sum()
        rows.append({
            "Måned": month_name,
            "Ture": ture if ture else 0,
            "Oplæring / Research": opl_res if not g[g["tour_code"].isin(SPECIAL_CODES)].empty else None,
            "Total DB-afvigelse": total,
        })

    summary = pd.DataFrame(rows)

    def _fmt_signed(v):
        if pd.isna(v):
            return "—"
        if v == 0:
            return "0"
        sign = "+" if v > 0 else "-"
        return f"{sign}{_fmt_kr(abs(v))}"

    def _color(row: pd.Series) -> list[str]:
        styles: list[str] = []
        for col, val in row.items():
            if col == "Måned":
                s = "font-weight:800; color:#1e3a5f; font-size:1rem;"
            elif col == "Total DB-afvigelse":
                if isinstance(val, (int, float)) and not pd.isna(val):
                    if val < 0:
                        s = "color:#c0392b; font-weight:800; font-size:1rem;"
                    elif val > 0:
                        s = "color:#1e8449; font-weight:800; font-size:1rem;"
                    else:
                        s = "color:#94a3b8; font-weight:700;"
                else:
                    s = ""
            elif col == "Oplæring / Research":
                s = "font-style:italic; font-weight:700;"
                if isinstance(val, (int, float)) and not pd.isna(val):
                    if val < 0:
                        s += " color:#c0392b;"
                    elif val > 0:
                        s += " color:#1e8449;"
                    else:
                        s += " color:#94a3b8;"
            else:  # Ture
                if isinstance(val, (int, float)) and not pd.isna(val):
                    if val < 0:
                        s = "color:#c0392b; font-weight:700;"
                    elif val > 0:
                        s = "color:#1e8449; font-weight:700;"
                    else:
                        s = "color:#94a3b8; font-weight:600;"
                else:
                    s = ""
            styles.append(s)
        return styles

    fmt_cols = {col: _fmt_signed for col in summary.columns if col != "Måned"}
    styled = summary.style.format(fmt_cols).apply(_color, axis=1)
    _row_h = 35
    _target_h = min(550, 60 + _row_h * len(summary))
    st.dataframe(styled, use_container_width=True, hide_index=True, height=_target_h)

    # === YTD-bokse under tabellen ===
    st.markdown("###### YTD (akkumuleret over valgte måneder)")
    ture_ytd = sum(r["Ture"] or 0 for r in rows)
    opl_ytd = sum((r["Oplæring / Research"] or 0) for r in rows)
    total_ytd = sum(r["Total DB-afvigelse"] or 0 for r in rows)

    def _signed(v: float) -> str:
        sign = "+" if v >= 0 else "-"
        return f"{sign}{_fmt_kr(abs(v))} kr."

    c1, c2, c3 = st.columns(3)
    c1.metric("Ture", _signed(ture_ytd))
    c2.metric("Oplæring / Research", _signed(opl_ytd))
    c3.metric("Total DB-afvigelse", _signed(total_ytd))


def _render_comparison_view(df_in: pd.DataFrame, month_nums: list[int]) -> None:
    """Tur-sammenligning: vælg turkoder, se diff pr. afgang på tværs af måneder."""
    # Find alle unikke "tur-familier" (kode uden trailing nummer)
    all_codes = sorted(df_in["tour_code"].dropna().unique().tolist())

    # Familie-grupper: ITTO2601, ITTO2602 -> familie "ITTO"
    import re as _re
    def _family(code: str) -> str:
        if code in SPECIAL_CODES:
            return code
        m = _re.match(r"^([A-Z]+)", code)
        return m.group(1) if m else code

    families = sorted({_family(c) for c in all_codes})

    col_sel, col_mode = st.columns([3, 1])
    with col_sel:
        selected_fams = st.multiselect(
            "Tur-familier (vælg én eller flere prefixer at sammenligne)",
            options=families,
            default=[],
            placeholder="fx ITTO, ESMV, NOSS ...",
        )
    with col_mode:
        view_mode = st.radio(
            "Visning",
            options=["Pr. afgang", "Sum pr. familie"],
            horizontal=False,
        )

    if not selected_fams:
        st.info("Vælg én eller flere tur-familier ovenfor for at se sammenligning.")
        return

    matching = df_in[df_in["tour_code"].apply(lambda c: _family(c) in selected_fams)]

    if matching.empty:
        st.info("Ingen afgange matchede de valgte familier i de valgte måneder.")
        return

    if view_mode == "Pr. afgang":
        # Pivot: rækker = turkode, kolonner = måned, værdier = diff
        pivot = matching.pivot_table(
            index="tour_code", columns="month_num",
            values="db_budget_diff", aggfunc="sum",
        )
        pivot.columns = [MONTH_ORDER[c - 1] for c in pivot.columns]
        pivot = pivot.reindex(columns=[MONTH_ORDER[m - 1] for m in month_nums if MONTH_ORDER[m - 1] in pivot.columns])
        pivot["YTD"] = pivot.sum(axis=1, min_count=1)
        pivot = pivot.sort_values("YTD", ascending=False)
    else:
        # Sum pr. familie
        matching = matching.copy()
        matching["family"] = matching["tour_code"].apply(_family)
        pivot = matching.pivot_table(
            index="family", columns="month_num",
            values="db_budget_diff", aggfunc="sum",
        )
        pivot.columns = [MONTH_ORDER[c - 1] for c in pivot.columns]
        pivot = pivot.reindex(columns=[MONTH_ORDER[m - 1] for m in month_nums if MONTH_ORDER[m - 1] in pivot.columns])
        pivot["YTD"] = pivot.sum(axis=1, min_count=1)
        pivot = pivot.sort_values("YTD", ascending=False)
        pivot.index.name = "Familie"

    def _fmt_or_blank(v):
        if pd.isna(v):
            return ""
        return _fmt_kr(v)

    def _color_cell(v):
        if pd.isna(v):
            return ""
        if v < 0:
            return "color:#c0392b; font-weight:700;"
        if v > 0:
            return "color:#1e8449; font-weight:700;"
        return "color:#94a3b8; font-weight:600;"

    fmt_dict = {col: _fmt_or_blank for col in pivot.columns}
    styled = (pivot.style.format(fmt_dict)
                  .map(_color_cell, subset=pivot.columns))
    st.dataframe(styled, use_container_width=True)


# === Tabs ===
tab_summary, tab_detail, tab_compare = st.tabs([
    "📊 Måneds-overblik",
    "📋 Detalje pr. måned",
    "🔎 Tur-sammenligning",
])

with tab_summary:
    st.caption("Sum pr. kategori og måned, uden tur-detaljer. YTD-kolonnen viser akkumuleret 2026.")
    _render_summary_view(df_filt, months_with_data)

with tab_detail:
    st.caption("Alle ture pr. måned side om side, opdelt i TOPAS / GREENLAND / VIETNAM.")
    _render_detail_view(df_filt, months_with_data)

with tab_compare:
    st.caption("Vælg tur-familier (fx ITTO, ESMV) og sammenlign resultaterne på tværs af måneder.")
    _render_comparison_view(df_filt, months_with_data)


with st.expander("ℹ Hvor kommer data fra?"):
    st.markdown(
        "Data hentes dagligt fra `K:\\OFFICE\\Operations\\Turregnskab\\Opfølgning\\"
        "Turomkostninger 2026.xls` af scriptet `scripts/sync_forecast.ps1` "
        "der kører via Windows Task Scheduler. Filen åbnes read-only via "
        "Excel COM-automation — ingen ændringer skrives tilbage.\n\n"
        "**Filter-regel:** kun rækker hvor kolonne B (Turkode), I (Realiseret DB) og "
        "M (DB-forskel) alle har værdi, regnes som realiserede ture og indlæses."
    )
