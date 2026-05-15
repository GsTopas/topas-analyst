"""
Streamlit page: Topas-katalog

Shows the authoritative list of Topas Fællesrejse-med-turleder products,
fetched from topas.dk's filtered search page. Indicates which tours have
godkendte konkurrenter (i approved_competitor_targets) og hvilke der mangler
mapping.

This is the foundation for the per-tour-code workflow — once we know what
Topas sells, we can systematically work through which tours need competitor
data captured.
"""

from __future__ import annotations

import os

import streamlit as st

# Password-gate — stopper page-rendering indtil korrekt adgangskode.
from topas_scraper._auth import require_auth  # noqa: E402
require_auth()

import pandas as pd

from topas_scraper import catalog_db
from topas_scraper.client import FirecrawlClient
from topas_scraper.db import (
    connect,
    upsert_topas_catalog,
    fetch_topas_catalog,
    add_topas_catalog_entry,
)
from topas_scraper.topas_catalog import (
    fetch_topas_catalog as fetch_catalog_from_web,
    scrape_tour_metadata,
    TOPAS_SEED_URLS,
)


st.title("📋 Topas-katalog")
st.caption(
    "Den autoritative liste over Topas Fællesrejse-med-turleder-ture. "
    "Vedligeholdt manuelt — tilføj nye URLs efterhånden som Topas tilføjer ture."
)


# ---------------------------------------------------------------------------
# Refresh + tilføj ny tur — to kolonner
# ---------------------------------------------------------------------------

tab_refresh, tab_add = st.tabs(["🔄 Refresh fra seed-liste", "➕ Tilføj ny tur"])

# Hent faktisk katalog-størrelse — så tællen følger DB i stedet for at vise
# den gamle hardcoded seed-liste-størrelse (manuelt tilføjede ture tæller med).
try:
    _conn = connect()
    _catalog_count = len(fetch_topas_catalog(_conn))
except Exception:
    _catalog_count = len(TOPAS_SEED_URLS)

with tab_refresh:
    st.markdown(
        f"Refresh henter metadata for alle **{_catalog_count} ture** "
        "i kataloget. Bruges når priser eller navne er ændret. "
        "Cost: ~$0.25 · Tid: ~3-4 min."
    )

    refresh_clicked = st.button(
        "🔄 Refresh katalog",
        type="primary",
        key="btn_refresh",
    )

    if refresh_clicked:
        with st.status("Refresher Topas-katalog...", expanded=True) as status:
            try:
                client = FirecrawlClient()

                log_messages: list[str] = []
                log_placeholder = st.empty()

                def progress(msg: str) -> None:
                    log_messages.append(msg)
                    tail = log_messages[-8:]
                    log_placeholder.text("\n".join(tail))

                # Brug URLs fra DB (inkl. manuelt tilføjede), ikke kun seed-listen.
                # Det sikrer at alle 50 ture refreshes — også dem brugeren har
                # tilføjet via "Tilføj ny tur"-fanen.
                _conn_for_urls = connect()
                _existing = fetch_topas_catalog(_conn_for_urls)
                _existing_urls = [r["url"] for r in _existing if r.get("url")]
                _all_urls = list(dict.fromkeys(_existing_urls + list(TOPAS_SEED_URLS)))
                tours = fetch_catalog_from_web(client, urls=_all_urls, on_progress=progress)

                if not tours:
                    status.update(label="Ingen ture hentet", state="error")
                    st.error(
                        "Refresh fejlede — alle scrapes returnerede tomt. "
                        "Tjek Firecrawl-credits og forbindelse."
                    )
                    st.stop()

                st.write(f"Gemmer {len(tours)} unikke ture i database...")
                conn = connect()
                new_count, updated_count, removed_count = upsert_topas_catalog(conn, tours)

                status.update(
                    label=(
                        f"Katalog opdateret: {new_count} nye, "
                        f"{updated_count} eksisterende, {removed_count} fjernet"
                    ),
                    state="complete",
                )
                msg = f"✓ {new_count} nye · {updated_count} refreshed"
                if removed_count > 0:
                    msg += f" · {removed_count} stale fjernet"
                st.success(msg)
            except Exception as e:
                status.update(label=f"Fejl ({type(e).__name__})", state="error")
                st.error(f"Fejl ({type(e).__name__}). Tjek server-logs.")
                if os.getenv("APP_DEBUG"):
                    st.exception(e)
                st.stop()


with tab_add:
    st.markdown(
        "Tilføj en specifik Topas-tur ved at indsætte URL'en. "
        "Systemet henter metadata og tilføjer turen til kataloget. "
        "Cost: ~$0.005 · Tid: ~5 sekunder."
    )

    new_url = st.text_input(
        "URL til ny Topas-tur",
        placeholder="https://www.topas.dk/tur-slug-here/",
        key="input_new_url",
    )

    add_clicked = st.button("➕ Tilføj tur", type="primary", key="btn_add")

    if add_clicked:
        if not new_url:
            st.warning("Indtast en URL først")
        elif not new_url.startswith("https://www.topas.dk/"):
            st.error("URL skal starte med https://www.topas.dk/")
        else:
            with st.status(f"Henter metadata fra {new_url}...", expanded=True) as status:
                try:
                    client = FirecrawlClient()

                    add_log: list[str] = []
                    add_placeholder = st.empty()

                    def add_progress(msg: str) -> None:
                        add_log.append(msg)
                        add_placeholder.text("\n".join(add_log[-5:]))

                    tour = scrape_tour_metadata(client, new_url, on_progress=add_progress)

                    if not tour:
                        status.update(label="Kunne ikke hente metadata", state="error")
                        st.error(
                            "Scrape fejlede. Tjek at URL'en er korrekt og at "
                            "Firecrawl har credits."
                        )
                        st.stop()

                    conn = connect()
                    action = add_topas_catalog_entry(conn, tour)

                    status.update(
                        label=f"Tur {action}: {tour['tour_name']}",
                        state="complete",
                    )
                    if action == "new":
                        st.success(
                            f"✓ Ny tur tilføjet: **{tour['tour_name']}** "
                            f"({tour.get('tour_code') or 'ingen kode'})"
                        )
                    else:
                        st.success(
                            f"✓ Tur opdateret: **{tour['tour_name']}** "
                            f"({tour.get('tour_code') or 'ingen kode'})"
                        )
                    st.info("Refresh siden for at se den i tabellen nedenfor.")
                except Exception as e:
                    status.update(label=f"Fejl ({type(e).__name__})", state="error")
                    st.error(f"Fejl ({type(e).__name__}). Tjek server-logs.")
                    if os.getenv("APP_DEBUG"):
                        st.exception(e)


# ---------------------------------------------------------------------------
# Catalog display
# ---------------------------------------------------------------------------

conn = connect()

# Auto-sync has_competitor_mapping mod approved_competitor_targets ved hver
# page-load. Tidligere blev kolonnen kun opdateret ved katalog-refresh, så den
# blev stale når reviewer godkendte nye konkurrenter. Cheap query (~10ms).
try:
    conn.execute(
        """
        UPDATE topas_catalog tc
        SET has_competitor_mapping = CASE
          WHEN EXISTS (
            SELECT 1 FROM approved_competitor_targets a
            WHERE a.topas_tour_code = tc.tour_code
          ) THEN 1 ELSE 0 END
        """
    )
    conn.commit()
except Exception:
    pass  # silent — bare vis det vi har

catalog_rows = fetch_topas_catalog(conn)

if not catalog_rows:
    st.info(
        "Katalogen er tom. Klik på **🔄 Refresh katalog** for at hente Topas's "
        "Fællesrejse-liste fra topas.dk."
    )
    st.stop()


# Convert to DataFrame for easy display + filtering
df = pd.DataFrame(
    [dict(row) for row in catalog_rows]
)

# ---------------------------------------------------------------------------
# Top-line metrics
# ---------------------------------------------------------------------------

mapped = int(df["has_competitor_mapping"].sum())
unmapped = len(df) - mapped

# Total approved competitor URLs på tværs af alle tours (kan være >1 pr tour-kode)
try:
    cat_conn = catalog_db.connect()
    approved_total = len(catalog_db.list_approved_targets(cat_conn))
except Exception:
    approved_total = 0

m1, m2, m3, m4 = st.columns(4)
m1.metric("Topas-ture i katalog", len(df))
m2.metric("Med konkurrent-mapping", mapped, help="Antal Topas-ture med mindst én godkendt konkurrent i Supabase")
m3.metric("Mangler mapping", unmapped, help="Ture i katalog uden konkurrent-mapping endnu — kør screening fra Tour-detalje")
m4.metric("Godkendte konkurrent-URLs", approved_total, help="Total antal godkendte konkurrent-targets på tværs af alle tours")


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

st.divider()
st.subheader("Katalog")

f1, f2, f3 = st.columns(3)

with f1:
    countries = ["Alle"] + sorted([c for c in df["country"].dropna().unique()])
    country_filter = st.selectbox("Land", countries)

with f2:
    mapping_filter = st.radio(
        "Mapping-status",
        ["Alle", "Med mapping", "Mangler mapping"],
        horizontal=True,
    )

with f3:
    search = st.text_input("Søg i tour-navn", placeholder="fx 'Vietnam' eller 'NPAP'")


# Apply filters
filtered = df.copy()
if country_filter != "Alle":
    filtered = filtered[filtered["country"] == country_filter]
if mapping_filter == "Med mapping":
    filtered = filtered[filtered["has_competitor_mapping"] == 1]
elif mapping_filter == "Mangler mapping":
    filtered = filtered[filtered["has_competitor_mapping"] == 0]
if search:
    s = search.lower()
    mask = (
        filtered["tour_name"].fillna("").str.lower().str.contains(s)
        | filtered["tour_code"].fillna("").str.lower().str.contains(s)
        | filtered["country"].fillna("").str.lower().str.contains(s)
    )
    filtered = filtered[mask]


# ---------------------------------------------------------------------------
# Table display
# ---------------------------------------------------------------------------

if filtered.empty:
    st.info("Ingen ture matcher filteret.")
else:
    # Add a status icon column
    def _status_icon(row):
        return "✅" if row["has_competitor_mapping"] == 1 else "⚠️"

    display_df = filtered.copy()
    display_df["Status"] = display_df.apply(_status_icon, axis=1)
    display_df = display_df[[
        "Status", "tour_code", "tour_name", "country",
        "duration_days", "from_price_dkk", "url",
    ]].rename(columns={
        "tour_code": "Kode",
        "tour_name": "Navn",
        "country": "Land",
        "duration_days": "Dage",
        "from_price_dkk": "Fra-pris (DKK)",
        "url": "URL",
    })

    st.dataframe(
        display_df,
        column_config={
            "URL": st.column_config.LinkColumn("URL", display_text="åbn ↗"),
            "Fra-pris (DKK)": st.column_config.NumberColumn(
                "Fra-pris (DKK)",
                format="%d kr.",
            ),
        },
        hide_index=True,
        use_container_width=True,
    )

    st.caption(
        f"{len(filtered)} ture vist (af {len(df)} i katalogen). "
        f"✅ = mindst én godkendt konkurrent (i Supabase) · "
        f"⚠️ = mangler godkendte konkurrenter — kør screening fra Tour-detalje"
    )


# ---------------------------------------------------------------------------
# Footer: next steps for unmapped tours
# ---------------------------------------------------------------------------

if unmapped > 0:
    with st.expander(f"📝 {unmapped} ture mangler konkurrent-mapping — næste skridt"):
        st.markdown("""
        Hver tur uden ✅ mangler godkendte konkurrenter i Supabase. Workflow:

        1. Vælg en tur uden mapping fra tabellen ovenfor (eller fra dropdown
           i **🔍 Tour-detalje**)
        2. Klik **🔍 Screen konkurrenter** — Python-pipelinen finder kandidater
           via Firecrawl Search + Claude
        3. Gennemgå kandidaterne i **📋 Review-kandidater** og godkend de
           relevante
        4. Godkendte targets ender i `approved_competitor_targets` i Supabase
           og fanges automatisk af næste ugentlig scrape (cron mandag morgen)
        """)
