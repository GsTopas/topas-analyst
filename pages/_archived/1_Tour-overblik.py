"""
Tour-overblik — Tabel over alle Topas-tours med n\u00e6rmeste konkurrent og spand.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Tour-overblik · Topas", page_icon="📊", layout="wide")

st.markdown("# 📊 Tour-overblik")
st.caption("Alle scrapede Topas-tours med nærmeste konkurrent, spænd, og flag")

json_path = Path("data/dashboard.json")
if not json_path.exists():
    st.warning(
        "Ingen scraped data fundet. Gå til **Tour-detalje** og kør første scrape, eller kør "
        "`python -m topas_scraper.cli scrape` lokalt."
    )
    st.stop()

try:
    data = json.loads(json_path.read_text(encoding="utf-8"))
except (json.JSONDecodeError, OSError) as exc:
    st.error(f"Kunne ikke læse dashboard.json: {exc}")
    st.stop()

tours = data.get("tours", [])
if not tours:
    st.info("dashboard.json indeholder ingen tours.")
    st.stop()

# ---------------------------------------------------------------------------
# Build dataframe
# ---------------------------------------------------------------------------
rows = []
for t in tours:
    flags = t.get("flags", {})
    flag_chips = []
    if flags.get("priceChange"):
        flag_chips.append("💰 Pris")
    if flags.get("slopeMismatch"):
        flag_chips.append("📉 Hældning")
    if flags.get("competitorSellout"):
        flag_chips.append("🚫 Udsolgt")

    topas_from = t.get("topasFromPrice")
    comp_from = t.get("competitorFromPrice")
    spread_from = (topas_from - comp_from) if (topas_from and comp_from) else None
    spread_pct = (spread_from / comp_from * 100) if (spread_from is not None and comp_from) else None

    topas_perdep = t.get("topasPerDep")
    comp_perdep = t.get("competitorPerDep")
    spread_perdep = (topas_perdep - comp_perdep) if (topas_perdep and comp_perdep) else None

    rows.append({
        "Tour": t.get("name", t["code"]),
        "Kode": t["code"],
        "Land": t.get("country", ""),
        "Dage": t.get("durationDays"),
        "Konkurrent": t.get("competitor", {}).get("operator", ""),
        "Tier": f"T{t.get('competitor', {}).get('tier', '?')}",
        "Topas fra": f"{topas_from:,} kr." if topas_from else "—",
        "Konkurrent fra": f"{comp_from:,} kr." if comp_from else "—",
        "Spænd (fra)": f"{spread_from:+,} kr." if spread_from is not None else "—",
        "Spænd %": f"{spread_pct:+.0f}%" if spread_pct is not None else "—",
        "Spænd (snit)": f"{spread_perdep:+,} kr." if spread_perdep is not None else "—",
        "Pair-pool": t.get("eligibleSetSize", "—"),
        "Flag": " · ".join(flag_chips) if flag_chips else "—",
    })

df = pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------
col_filter, col_search = st.columns([1, 2])

with col_filter:
    countries = ["Alle"] + sorted(df["Land"].unique().tolist())
    selected_country = st.selectbox("Region", countries)

with col_search:
    search = st.text_input("Søg (tour-navn eller kode)", placeholder="fx Madeira eller PTMD")

filtered = df.copy()
if selected_country != "Alle":
    filtered = filtered[filtered["Land"] == selected_country]
if search:
    mask = (
        filtered["Tour"].str.contains(search, case=False, na=False)
        | filtered["Kode"].str.contains(search, case=False, na=False)
    )
    filtered = filtered[mask]

# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------
st.dataframe(
    filtered,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Tour": st.column_config.TextColumn(width="medium"),
        "Kode": st.column_config.TextColumn(width="small"),
        "Land": st.column_config.TextColumn(width="small"),
        "Dage": st.column_config.NumberColumn(width="small"),
        "Konkurrent": st.column_config.TextColumn(width="medium"),
        "Tier": st.column_config.TextColumn(width="small"),
        "Topas fra": st.column_config.TextColumn(width="small"),
        "Konkurrent fra": st.column_config.TextColumn(width="small"),
        "Spænd (fra)": st.column_config.TextColumn(width="small"),
        "Spænd %": st.column_config.TextColumn(width="small"),
        "Spænd (snit)": st.column_config.TextColumn(width="small"),
        "Pair-pool": st.column_config.NumberColumn(width="small"),
        "Flag": st.column_config.TextColumn(width="medium"),
    },
)

st.caption(f"Viser {len(filtered)} af {len(df)} tours")

# ---------------------------------------------------------------------------
# Quick links to detail pages
# ---------------------------------------------------------------------------
st.divider()
st.markdown("### Hurtig tilgang")
st.markdown(
    "Klik en tour-kode for at åbne detalje-siden med konkurrent-picker og live scrape:"
)

# Streamlit's page navigation — we'll use query params as a soft link convention
cols = st.columns(min(len(filtered), 6) or 1)
for i, (_, row) in enumerate(filtered.iterrows()):
    with cols[i % len(cols)]:
        if st.button(f"🔍 {row['Kode']}", key=f"goto_{row['Kode']}", use_container_width=True):
            st.session_state["selected_tour_code"] = row["Kode"]
            st.switch_page("pages/2_🔍_Tour-detalje.py")
