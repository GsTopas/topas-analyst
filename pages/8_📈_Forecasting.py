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
col1, col2, col3 = st.columns(3)
with col1:
    tour_prefix = st.text_input(
        "Filtrer turkoder (prefix)",
        value="",
        placeholder="fx ESMV, NOSS, IVXX ...",
        help="Vis kun turkoder der starter med denne tekst. Tom = alle."
    )
with col2:
    only_realiseret = st.checkbox(
        "Kun realiserede ture",
        value=True,
        help="Kun rækker hvor Realiseret DB er populeret (kilde-filter er allerede aktivt — denne option er en ekstra spærre)."
    )
with col3:
    show_pivot = st.radio(
        "Visning",
        options=["Per måned (pivot)", "Flad tabel"],
        index=0,
        horizontal=True,
    )


# Apply filtre
df_filt = df.copy()
if tour_prefix.strip():
    df_filt = df_filt[df_filt["tour_code"].str.startswith(tour_prefix.strip().upper(), na=False)]
if only_realiseret:
    df_filt = df_filt[df_filt["realiseret_db"].notna()]


# === Pr. måneds-total ===
st.markdown("### Total DB-forskel pr. måned")
monthly = (
    df_filt.groupby(["month_num", "month"], as_index=False)["db_budget_diff"]
    .sum()
    .sort_values("month_num")
)
monthly["Total"] = monthly["db_budget_diff"].apply(
    lambda v: f"{v:,.0f} kr.".replace(",", ".") if pd.notna(v) else "—"
)
monthly_display = monthly[["month", "Total"]].rename(columns={"month": "Måned"})
st.dataframe(monthly_display, use_container_width=True, hide_index=True)

st.divider()


# === Detalje-tabel ===
if show_pivot.startswith("Per måned"):
    st.markdown("### Pivot — turkode × måned")
    st.caption("Celleværdier = DB-forskel ift. budget i kr. Blank = turen er ikke i den måned.")

    pivot = df_filt.pivot_table(
        index="tour_code",
        columns="month_num",
        values="db_budget_diff",
        aggfunc="sum",
    )

    # Omdøb kolonner til måneds-navne
    pivot.columns = [MONTH_ORDER[c - 1] for c in pivot.columns]
    # Sortér kolonner i kalender-rækkefølge
    pivot = pivot.reindex(columns=[m for m in MONTH_ORDER if m in pivot.columns])

    # Tilføj sum-kolonne pr. tur
    pivot["Total"] = pivot.sum(axis=1, min_count=1)
    pivot = pivot.sort_values("Total", ascending=False)

    # Format som kr.
    def _fmt(v):
        if pd.isna(v):
            return ""
        return f"{v:,.0f}".replace(",", ".")

    st.dataframe(
        pivot.style.format(_fmt).background_gradient(
            cmap="RdYlGn",
            subset=[c for c in pivot.columns if c != "Total"],
            vmin=-50000, vmax=50000,
        ),
        use_container_width=True,
    )

else:
    st.markdown("### Alle rækker")
    df_display = df_filt.copy()
    df_display["Hjemkomst"] = pd.to_datetime(df_display["homecoming_date"], errors="coerce").dt.strftime("%d. %b %Y")
    df_display["Budget DB"] = df_display["budget_db"].apply(
        lambda v: f"{v:,.0f}".replace(",", ".") if pd.notna(v) else "—"
    )
    df_display["Realiseret DB"] = df_display["realiseret_db"].apply(
        lambda v: f"{v:,.0f}".replace(",", ".") if pd.notna(v) else "—"
    )
    df_display["Δ DB"] = df_display["db_budget_diff"].apply(
        lambda v: f"{v:+,.0f}".replace(",", ".") if pd.notna(v) else "—"
    )
    df_display["Δ Pax"] = df_display["pax_diff"]
    df_display["Δ DG"] = df_display["dg_diff"].apply(
        lambda v: f"{v:+.1f}".replace(".", ",") if pd.notna(v) else "—"
    )

    st.dataframe(
        df_display[["month", "tour_code", "Hjemkomst", "Budget DB", "Realiseret DB", "Δ DB", "Δ Pax", "Δ DG"]]
        .rename(columns={"month": "Måned", "tour_code": "Turkode"}),
        use_container_width=True,
        hide_index=True,
    )


with st.expander("ℹ Hvor kommer data fra?"):
    st.markdown(
        "Data hentes dagligt fra `K:\\OFFICE\\Operations\\Turregnskab\\Opfølgning\\"
        "Turomkostninger 2026.xls` af scriptet `scripts/sync_forecast.ps1` "
        "der kører via Windows Task Scheduler. Filen åbnes read-only via "
        "Excel COM-automation — ingen ændringer skrives tilbage.\n\n"
        "**Filter-regel:** kun rækker hvor kolonne B (Turkode), I (Realiseret DB) og "
        "M (DB-forskel) alle har værdi, regnes som realiserede ture og indlæses."
    )
