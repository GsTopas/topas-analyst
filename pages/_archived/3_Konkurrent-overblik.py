"""
Konkurrent-overblik — Alle konkurrenter med portef\u00f8lje, antal afgange og placering.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Konkurrent-overblik · Topas", page_icon="🏢", layout="wide")

st.markdown("# 🏢 Konkurrent-overblik")
st.caption("Alle konkurrenter — antal afgange, prisniveau, og holding-relation")

json_path = Path("data/dashboard.json")
if not json_path.exists():
    st.warning("Ingen scraped data fundet. Gå til **Tour-detalje** og kør første scrape.")
    st.stop()

try:
    data = json.loads(json_path.read_text(encoding="utf-8"))
except (json.JSONDecodeError, OSError) as exc:
    st.error(f"Kunne ikke læse dashboard.json: {exc}")
    st.stop()

competitors = data.get("competitors", [])
if not competitors:
    st.info("Ingen konkurrenter i dashboard.json endnu.")
    st.stop()

# ---------------------------------------------------------------------------
# Build dataframe
# ---------------------------------------------------------------------------
rows = []
for c in competitors:
    deps = c.get("departures", [])
    sellouts = sum(1 for d in deps if d.get("status") == "Udsolgt")
    fromp = c.get("fromPrice")
    avgp = c.get("perDepAvg")

    rows.append({
        "Operatør": c["operator"],
        "Holding": c.get("holding") or "—",
        "Segment": c.get("segment") or "",
        "Tour-kode": c.get("competesWith") or "—",
        "Tier": f"T{c.get('tierForPTMD', '?')}",
        "Tour-navn": c.get("tourName", ""),
        "Land": c.get("country", ""),
        "Afgange": c.get("departureCount", 0),
        "Udsolgt": sellouts,
        "Fra-pris": f"{fromp:,} kr." if fromp else "—",
        "Snit-pris": f"{avgp:,} kr." if avgp else "—",
    })

df = pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------
col_a, col_b, col_c = st.columns([1, 1, 2])

with col_a:
    holdings = ["Alle"] + sorted(set(r["Holding"] for r in rows if r["Holding"] != "—"))
    selected_holding = st.selectbox("Holding", holdings)

with col_b:
    tiers = ["Alle"] + sorted(df["Tier"].unique().tolist())
    selected_tier = st.selectbox("Tier", tiers)

with col_c:
    show_only_eligible = st.checkbox("Kun eligible (>0 afgange)", value=False)

# Apply filters
filtered = df.copy()
if selected_holding != "Alle":
    filtered = filtered[filtered["Holding"] == selected_holding]
if selected_tier != "Alle":
    filtered = filtered[filtered["Tier"] == selected_tier]
if show_only_eligible:
    filtered = filtered[filtered["Afgange"] > 0]

# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------
st.dataframe(
    filtered,
    use_container_width=True,
    hide_index=True,
)

st.caption(f"Viser {len(filtered)} af {len(df)} konkurrenter")

# ---------------------------------------------------------------------------
# Aller Leisure aggregate
# ---------------------------------------------------------------------------
st.divider()
st.markdown("### Aller Leisure portefølje")

aller_brands = [c for c in competitors if c.get("holding") == "Aller Leisure"]
if not aller_brands:
    st.markdown("_Ingen Aller Leisure-brands i den nuværende portefølje._")
else:
    total_deps = sum(c.get("departureCount", 0) for c in aller_brands)
    eligible = sum(1 for c in aller_brands if c.get("departureCount", 0) > 0)

    col_x, col_y, col_z = st.columns(3)
    with col_x:
        st.metric("Brands i portefølje", len(aller_brands))
    with col_y:
        st.metric("Eligible brands", eligible)
    with col_z:
        st.metric("Afgange total", total_deps)

    st.markdown(
        "Aller Leisure A/S ejer "
        + ", ".join(f"**{c['operator']}**" for c in aller_brands)
        + ". Brands tracker hver for sig (jf. methodology.md §1.4) — ikke aggregeret."
    )
