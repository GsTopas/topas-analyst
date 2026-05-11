"""
Topas Konkurrencedygtig Prisintelligens — Streamlit-app

Hovedindgang. Streamlit auto-discoverer filerne i `pages/` og bygger sidebar-navigation.
Denne fil viser snapshot-metadata og en kort intro.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

# Load .env BEFORE any module that reads os.environ — so FIRECRAWL_API_KEY
# and ANTHROPIC_API_KEY are available to the scraper / vision_extractor.
# Must run before page modules import topas_scraper.runner.
load_dotenv()

# ---------------------------------------------------------------------------
# Page config — skal være den første Streamlit-kald i appen
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Topas Prisintelligens",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Password-gate — stopper page-rendering indtil korrekt adgangskode.
# Hvis APP_PASSWORD ikke er sat (lokalt dev), springes auth over.
from topas_scraper._auth import require_auth  # noqa: E402
require_auth()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
col_title, col_meta = st.columns([3, 1])

with col_title:
    st.markdown("# Topas")
    st.markdown(
        "**Konkurrencedygtig prisintelligens** — "
        "ugentligt overblik over Topas-tours sammenlignet med danske konkurrenter"
    )

with col_meta:
    # Snapshot meta — læses fra dashboard.json hvis den findes
    json_path = Path("data/dashboard.json")
    if json_path.exists():
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
            ts_raw = data.get("snapshotTakenAt", "")
            run_id = data.get("snapshotRunId", "")[:8]
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                ts_pretty = ts.strftime("%d. %b %Y · kl. %H:%M")
            except (ValueError, TypeError):
                ts_pretty = ts_raw
            st.markdown(
                f"<div style='text-align:right; font-size:12px; color:#666;'>"
                f"Snapshot <code>{run_id}</code><br>"
                f"<strong>{ts_pretty}</strong></div>",
                unsafe_allow_html=True,
            )
        except (json.JSONDecodeError, OSError):
            st.warning("dashboard.json kunne ikke læses")
    else:
        st.info("Ingen scraped data endnu — kør første scrape", icon="ℹ️")

st.divider()

# ---------------------------------------------------------------------------
# Intro / quick access
# ---------------------------------------------------------------------------
st.markdown("### Værktøjet")

st.markdown(
    """
Tre sider — vælg i sidebaren:

- **📊 Tour-overblik** — alle Topas-tours med nærmeste konkurrent, spænd, og flag
- **🔍 Tour-detalje** — dyk ned i én tour, sammenlign mod alle konkurrenter, kør **live scrape**
- **🏢 Konkurrent-overblik** — alle konkurrenter med portefølje-overlap og placering

Værktøjet bygger på den methodologi der ligger i `methodology.md` og `taxonomy.md`. Sammenligninger
er kun gyldige inden for `tour_format = Fællesrejse` — segmenter som 30-50 år, Individuelle, og
Privat gruppe er ekskluderet.
"""
)

st.divider()

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
st.markdown("### Dækning")

if json_path.exists():
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
        tours = data.get("tours", [])
        comps = data.get("competitors", [])

        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.metric("Topas-tours scraped", len(tours))
        with col_b:
            eligible_comps = [c for c in comps if c.get("departureCount", 0) > 0]
            st.metric("Eligible konkurrenter", len(eligible_comps))
        with col_c:
            total_deps = sum(len(t.get("departures", [])) for t in tours)
            total_comp_deps = sum(c.get("departureCount", 0) for c in comps)
            st.metric("Datapunkter total", total_deps + total_comp_deps)

        st.markdown(
            "**Live tours:** "
            + ", ".join(f"`{t['code']}`" for t in tours)
            if tours
            else "_Ingen tours scraped endnu._"
        )
    except (json.JSONDecodeError, OSError) as exc:
        st.error(f"Kunne ikke læse dashboard.json: {exc}")
else:
    st.markdown("_Klik **Tour-detalje** i sidebaren og kør første scrape._")

st.divider()
st.caption(
    "Data scrapet via Firecrawl. Eligible konkurrenter er Fællesrejse-format med "
    "publicerede afgange. Per-departure spread er det metodologisk korrekte sammenligningsgrundlag "
    "(jf. methodology.md §2.bis), ikke fra-prisen."
)
