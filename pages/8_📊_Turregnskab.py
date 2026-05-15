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

# Korte labels som vises i Detalje-tab (matches brugerens template).
CATEGORY_SHORT = {
    "TOPAS": "Topas",
    "GREENLAND BY TOPAS": "GBT",
    "VIETNAM BY TOPAS": "VBT",
}


def _build_month_rows(
    g: pd.DataFrame,
    cat_slot_sizes: dict[str, int] | None = None,
) -> list[tuple[str, str]]:
    """Returnér liste af (tur-celle, diff-celle) for én måned med kategori-
    rubrikker (Topas/GBT/VBT), sub-totaler og spacer mellem kategorier.

    Hvis cat_slot_sizes er givet, padder vi hver kategoris tur-blok til
    præcis den størrelse — så kategori-rubrikker aligner horisontalt på
    tværs af måneder uanset hvor mange ture hver måned har i kategorien."""
    rows: list[tuple[str, str]] = []
    cats: dict[str, pd.DataFrame] = {
        c: g[g["tour_code"].apply(_categorize) == c]
        for c in CATEGORY_ORDER
    }
    for cat_name in CATEGORY_ORDER:
        sub = cats[cat_name].copy()
        sub["_special"] = sub["tour_code"].isin(SPECIAL_CODES)
        sub = sub.sort_values(
            ["_special", "homecoming_date", "tour_code"],
            na_position="last",
        )

        slot_size = cat_slot_sizes[cat_name] if cat_slot_sizes else len(sub)
        if slot_size == 0:
            continue

        short = CATEGORY_SHORT[cat_name]
        rows.append((short, ""))  # kategori-header
        for _, r in sub.iterrows():
            rows.append((r["tour_code"], _fmt_kr(r["db_budget_diff"])))

        # Pad med tomme raekker hvis maaneden har faerre ture end max-slot
        padding = slot_size - len(sub)
        for _ in range(padding):
            rows.append(("", ""))

        cat_total = sub["db_budget_diff"].sum() if not sub.empty else 0
        rows.append((f"{short} total", _fmt_kr(cat_total)))
        rows.append(("", ""))  # spacer mellem kategorier

    while rows and rows[-1] == ("", ""):
        rows.pop()
    return rows


# Custom CSS — fed paa headers + tal paa tvers af de 3 faner.
# Streamlit Arrow-renderer har forskellige DOM-strukturer i forskellige
# versioner; vi targetterer alle kendte selectors for at sikre at fed rammer.
st.markdown(
    """
    <style>
    /* === Headers (top-level + sub-level) === */
    [data-testid="stDataFrame"] thead th,
    [data-testid="stDataFrame"] th,
    div[data-testid="stDataFrameResizable"] thead th,
    .stDataFrame thead th,
    [role="columnheader"],
    [role="columnheader"] *,
    [role="columnheader"] span,
    [role="columnheader"] div,
    [data-testid="stDataFrame"] [role="columnheader"] {
        font-weight: 900 !important;
        color: #0f172a !important;
        background-color: #e2e8f0 !important;
        font-size: 0.95rem !important;
    }
    /* Glide-data-grid (Streamlit's underliggende grid-engine) headers */
    [data-testid="stDataFrame"] canvas + div [role="columnheader"],
    [data-testid="stDataFrame"] [role="row"][aria-rowindex="1"] *,
    [data-testid="stDataFrame"] [role="row"][aria-rowindex="2"] * {
        font-weight: 900 !important;
        color: #0f172a !important;
    }
    /* === Body cells === */
    [data-testid="stDataFrame"] tbody td,
    [data-testid="stDataFrame"] td {
        font-size: 0.9rem !important;
    }
    /* === Tab-labels === */
    button[data-baseweb="tab"] p,
    .stTabs [data-baseweb="tab"] {
        font-weight: 700 !important;
        font-size: 0.95rem !important;
    }
    /* === Metric-bokse (st.metric) ovre under tabellen === */
    [data-testid="stMetricLabel"] {
        font-weight: 700 !important;
    }
    [data-testid="stMetricValue"] {
        font-weight: 800 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

def _render_detail_view(df_in: pd.DataFrame, month_nums: list[int]) -> None:
    """Detalje-tabel: side-om-side maaneds-kolonner med Topas/GBT/VBT-rubrikker.

    Hver maaned har sit eget naturlige flow - INGEN padding, INGEN horisontal
    alignment paa tvers af maaneder. Kategori-rubrikker er bare almindelige
    rakker med fed tekst i Tur-kolonnen (ikke en row-spanning styling)."""
    month_rows: dict[int, list[tuple[str, str]]] = {}
    for m_num in month_nums:
        g = df_in[df_in["month_num"] == m_num]
        month_rows[m_num] = _build_month_rows(g, cat_slot_sizes=None)  # ingen padding!

    max_rows = max(len(r) for r in month_rows.values()) if month_rows else 0

    columns = []
    data: dict[tuple, list] = {}
    month_totals: dict[int, float] = {}
    # Map fra header-streng tilbage til month_num (saa total_row-lookup virker)
    header_to_month: dict[str, int] = {}
    for m_num in month_nums:
        month_name = MONTH_ORDER[m_num - 1]
        g = df_in[df_in["month_num"] == m_num]
        total = g["db_budget_diff"].sum()
        month_totals[m_num] = total

        # Header: Maaned + total + Opl/Res-parentes
        sign = "+" if total >= 0 else "-"
        header = f"{month_name}  ·  {sign}{_fmt_kr(abs(total))} kr."
        opl_res_g = g[g["tour_code"].isin(SPECIAL_CODES)]
        if not opl_res_g.empty:
            opl_res = opl_res_g["db_budget_diff"].sum()
            or_sign = "+" if opl_res >= 0 else "-"
            header += f"  (Opl/Res: {or_sign}{_fmt_kr(abs(opl_res))} kr.)"
        header_to_month[header] = m_num

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
        m_num = header_to_month[header]
        if sub == "Tur":
            total_row[(header, sub)] = "Total"
        else:
            total_row[(header, sub)] = _fmt_kr(month_totals[m_num])
    table.loc[len(table)] = pd.Series(total_row)

    cat_labels = set(CATEGORY_SHORT.values())  # {"Topas", "GBT", "VBT"}

    def _style(row: pd.Series) -> list[str]:
        """Styling er PER CELLE, ikke per ROW.
        Hver maaneds Tur-kolonne kan have en kategori-label paa en raekke
        hvor en anden maaneds Tur-kolonne har en almindelig turkode.
        Vi kan altsaa ikke applye row-spanning baggrund - styling laeses
        for hver celle individuelt."""
        styles = []
        for (_h, sub), val in row.items():
            s = ""
            val_str = str(val).strip() if val is not None else ""

            if sub == "Tur":
                # Tur-kolonnen: kategori-labels og 'X total' i fed
                if val_str in cat_labels:
                    s = "font-weight:700; color:#1e3a5f;"
                elif val_str.endswith(" total") and val_str != "Total":
                    s = "font-weight:700; color:#1e3a5f;"
                elif val_str == "Total":
                    s = "font-weight:800; color:#7c2d12;"
            else:
                # DB-kolonnen
                if val_str.replace(".", "").replace("-", "").replace(",", "").isdigit() or val_str == "0":
                    # Total-row: find tilhoerende tur-celle for at se om denne row er Total
                    tour_val = ""
                    for (_h2, s2), v2 in row.items():
                        if s2 == "Tur":
                            tour_val = str(v2).strip()
                            break
                    if tour_val == "Total":
                        s = "font-weight:800; color:#7c2d12;"
                    elif tour_val.endswith(" total"):
                        s = "font-weight:700; color:#1e3a5f;"
                    elif val_str.startswith("-"):
                        s = "color:#c0392b;"
                    elif val_str != "0":
                        s = "color:#1e8449;"
            styles.append(s)
        return styles

    styled = table.style.apply(_style, axis=1)
    _row_h, _header_h = 32, 70
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
                # Foerste kolonne: fed
                s = "font-weight:700; color:#1e3a5f;"
            elif col == "Total DB-afvigelse":
                # Total: ikke fed, kun farvet
                if isinstance(val, (int, float)) and not pd.isna(val):
                    if val < 0:
                        s = "color:#c0392b;"
                    elif val > 0:
                        s = "color:#1e8449;"
                    else:
                        s = "color:#94a3b8;"
                else:
                    s = ""
            elif col == "Oplæring / Research":
                # Italic, ikke fed
                s = "font-style:italic;"
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
                        s = "color:#c0392b;"
                    elif val > 0:
                        s = "color:#1e8449;"
                    else:
                        s = "color:#94a3b8;"
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
        pivot.index.name = "Turkode"
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
            return "color:#c0392b;"
        if v > 0:
            return "color:#1e8449;"
        return "color:#94a3b8;"

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
    st.caption(
        "Alle ture pr. måned side om side, opdelt i kategorier: "
        "**Topas** · **GBT** (Greenland by Topas) · **VBT** (Vietnam by Topas)."
    )
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
