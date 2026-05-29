"""
Discovery — find tours competitors have that Topas doesn't.

Workflow:
  1. Vælg konkurrent (eller indtast nyt domain)
  2. Klik "Kør discovery"
  3. Backend henter alle tour-URLs, scraper + klassificerer hver,
     filtrerer på ICP, gap-analyserer mod Topas-katalog
  4. Resultat-tabel: rank, tour-name, country, activity, duration,
     departure-count, gap-grund, lærings-score
  5. "Godkend som lead" → upsert til n8n_candidates til manuel review
"""
from __future__ import annotations

import threading
import time

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Discovery · Topas", page_icon="🔭", layout="wide")

# Password-gate
from topas_scraper._auth import require_auth  # noqa: E402
require_auth()


COMPETITORS: dict[str, dict] = {
    "Gjøa Tours": {
        "domain": "gjoa.dk",
        "homepage": "https://gjoa.dk",
        "sitemap": "https://gjoa.dk/sitemap.xml",
    },
    "Jysk Rejsebureau": {
        "domain": "jysk-rejsebureau.dk",
        "homepage": "https://www.jysk-rejsebureau.dk",
        "sitemap": "https://www.jysk-rejsebureau.dk/sitemap.xml",
    },
    "Smilrejser": {
        "domain": "smilrejser.dk",
        "homepage": "https://smilrejser.dk",
        "sitemap": "https://smilrejser.dk/sitemap.xml",
    },
    "Albatros Travel": {
        "domain": "albatros.dk",
        "homepage": "https://www.albatros.dk",
        "sitemap": "https://www.albatros.dk/sitemap.xml",
    },
    "Viktors Farmor": {
        "domain": "viktorsfarmor.dk",
        "homepage": "https://www.viktorsfarmor.dk",
        "sitemap": "https://www.viktorsfarmor.dk/sitemap.xml",
    },
    "Ruby Rejser": {
        "domain": "ruby-rejser.dk",
        "homepage": "https://ruby-rejser.dk",
        "sitemap": "https://ruby-rejser.dk/sitemap.xml",
    },
    "Stjernegaard Rejser": {
        "domain": "stjernegaard-rejser.dk",
        "homepage": "https://www.stjernegaard-rejser.dk",
        "sitemap": "https://www.stjernegaard-rejser.dk/sitemap.xml",
    },
    "Fyrholt Rejser": {
        "domain": "fyrholtrejser.dk",
        "homepage": "https://fyrholtrejser.dk",
        "sitemap": "https://fyrholtrejser.dk/sitemap.xml",
    },
    "Vagabond Tours": {
        "domain": "vagabondtours.dk",
        "homepage": "http://www.vagabondtours.dk",
        "sitemap": None,
    },
    "Kipling Travel": {
        "domain": "kiplingtravel.dk",
        "homepage": "https://www.kiplingtravel.dk",
        "sitemap": "https://www.kiplingtravel.dk/sitemap.xml",
    },
    "Best Travel": {
        "domain": "besttravel.dk",
        "homepage": "https://www.besttravel.dk",
        "sitemap": "https://www.besttravel.dk/sitemap.xml",
    },
    "Nilles & Gislev": {
        "domain": "nillesgislev.dk",
        "homepage": "https://nillesgislev.dk",
        "sitemap": None,
    },
}


st.markdown("# 🔭 Discovery")
st.caption(
    "Find ture konkurrenter har — som Topas IKKE har. "
    "Filtrerer på Topas ICP (fixed-departure group tours med dansk rejseleder, "
    "vandring/cykling, 4-25 dage). Læring-base: 185 historiske afvisninger."
)

col_op, col_max, col_par = st.columns([3, 1, 1])
with col_op:
    op_choice = st.selectbox(
        "Konkurrent",
        options=list(COMPETITORS.keys()) + ["+ Custom domain"],
        index=0,
        help="Vælg en af de 12 kendte konkurrenter, eller indtast et nyt domain."
    )

if op_choice == "+ Custom domain":
    custom_home = st.text_input("Custom homepage URL", placeholder="https://example.dk")
    op_name = custom_home.replace("https://", "").replace("http://", "").split("/")[0] or "Custom"
    op_meta = {"domain": op_name, "homepage": custom_home, "sitemap": None}
else:
    op_name = op_choice
    op_meta = COMPETITORS[op_choice]

with col_max:
    max_urls = st.number_input(
        "Max URLs",
        min_value=5, max_value=200, value=50, step=5,
        help="Cap på antal URLs at scrape (~1 Firecrawl credit + 1 Claude-kald pr. URL)"
    )

with col_par:
    parallelism = st.number_input(
        "Workers",
        min_value=1, max_value=20, value=8, step=1,
        help="Parallelism for Firecrawl-scrapes. Firecrawl Hobby: 5, Standard: 50."
    )

st.divider()


if "discovery_result" not in st.session_state:
    st.session_state["discovery_result"] = None

run_clicked = st.button("🔭 Kør discovery", type="primary", use_container_width=True)

if run_clicked:
    if not op_meta.get("homepage"):
        st.error("Mangler homepage-URL.")
        st.stop()

    log_lines: list[str] = []
    log_lock = threading.Lock()
    log_placeholder = st.empty()

    def _emit(msg: str) -> None:
        with log_lock:
            log_lines.append(f"`{time.strftime('%H:%M:%S')}` {msg}")
            log_placeholder.markdown("\n\n".join(log_lines[-20:]))

    with st.status(f"Discovery: {op_name}", expanded=True) as status:
        try:
            from topas_scraper.competitor_discovery import run_discovery
            result = run_discovery(
                operator=op_name,
                homepage_url=op_meta["homepage"],
                sitemap_url=op_meta.get("sitemap"),
                progress_callback=_emit,
                max_urls=int(max_urls),
                parallelism=int(parallelism),
            )
            st.session_state["discovery_result"] = {
                "operator": op_name,
                "domain": op_meta["domain"],
                "result": result,
                "completed_at": time.strftime("%H:%M:%S"),
            }
            status.update(label=f"✓ Discovery færdig: {len(result['gaps'])} gap-ture", state="complete")
        except Exception as exc:
            status.update(label=f"✗ Discovery fejlede: {type(exc).__name__}", state="error")
            st.exception(exc)


res_meta = st.session_state.get("discovery_result")
if res_meta and res_meta["result"]:
    result = res_meta["result"]
    op = res_meta["operator"]
    domain = res_meta["domain"]
    gaps = result["gaps"]

    st.markdown(f"## Resultat for **{op}**")

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("URLs fundet", result["urls_discovered"])
    m2.metric(
        "Allerede mappet",
        result["stats"].get("already_mapped_filtered", 0),
        help="Fjernet før scrape: ture vi allerede tracker via approved_competitor_targets"
    )
    m3.metric("Klassificeret", result["tours_classified"])
    m4.metric("ICP-pass", result["icp_passing"])
    m5.metric("Gap-ture", len(gaps))
    m6.metric("Metode", result["stats"]["method"])

    if result["stats"].get("max_urls_capped"):
        st.warning(
            f"Capped scrape ved {max_urls} URLs (af {result['urls_discovered']} fundet). "
            "Bump 'Max URLs' op for fuld dækning."
        )

    if result["stats"].get("errors"):
        with st.expander(f"⚠ {len(result['stats']['errors'])} fejl under scrape"):
            for err in result["stats"]["errors"][:15]:
                st.code(err, language="text")

    if not gaps:
        st.info(
            "Ingen gap-ture fundet. Det kan betyde at:\n"
            "  - Konkurrenten har ingen ICP-relevant indhold\n"
            "  - Alle deres ICP-ture overlapper med Topas-kataloget\n"
            "  - Sitemap/discovery returnerede for få URLs (bump Max URLs)"
        )
        st.stop()

    st.markdown("### Gap-ture — ranked by score")
    st.caption(
        "Score = afgange-næste-12mdr × country-priority − afvist-malus. "
        "Højere score = mere strategisk værdifuld."
    )

    df = pd.DataFrame([
        {
            "Score": round(g.score, 1),
            "Tur": g.tour.tour_name[:60],
            "Land": g.tour.country or "?",
            "Aktivitet": g.tour.activity or "?",
            "Dage": g.tour.duration_days or "?",
            "Afgange (12mdr)": g.tour.departure_count_next_12mo,
            "Næste afgang": g.tour.next_departure or "—",
            "Fra-pris (kr)": f"{g.tour.from_price_dkk:,}".replace(",", ".") if g.tour.from_price_dkk else "—",
            "Gap-grund": g.gap_reason,
            "Lignende afvist": g.rejected_similar_count,
            "URL": g.tour.url,
        }
        for g in gaps
    ])

    def _color_score(v):
        if pd.isna(v):
            return ""
        if v >= 12:
            return "background-color: rgba(46, 139, 87, 0.5); font-weight:700;"
        if v >= 6:
            return "background-color: rgba(46, 139, 87, 0.3);"
        if v < 1:
            return "background-color: rgba(192, 57, 43, 0.3);"
        return ""

    styled = df.style.map(_color_score, subset=["Score"])
    st.dataframe(styled, use_container_width=True, hide_index=True, height=600)

    st.markdown("### Detalje + godkend som lead")
    selected_url = st.selectbox(
        "Vælg en tur for at se detaljer + godkende",
        options=[""] + [g.tour.url for g in gaps],
        format_func=lambda u: "(vælg ...)" if not u else next(
            (f"{g.tour.tour_name[:60]} — score {g.score:.1f}" for g in gaps if g.tour.url == u),
            u
        ),
    )

    if selected_url:
        selected_gap = next((g for g in gaps if g.tour.url == selected_url), None)
        if selected_gap:
            t = selected_gap.tour
            c1, c2 = st.columns([2, 1])
            with c1:
                st.markdown(f"**{t.tour_name}**")
                st.markdown(f"- URL: [{t.url}]({t.url})")
                st.markdown(f"- {t.country} · {t.activity} · {t.duration_days} dage")
                st.markdown(
                    f"- {t.departure_count_next_12mo} afgange næste 12 mdr "
                    f"(næste: {t.next_departure or '—'})"
                )
                if t.from_price_dkk:
                    st.markdown(f"- Frapris: {t.from_price_dkk:,} kr.".replace(",", "."))
                st.markdown(f"- **Gap-grund**: {selected_gap.gap_reason}")
                if t.classifier_notes:
                    st.caption(f"Classifier: {t.classifier_notes}")
                if selected_gap.rejected_similar_count:
                    st.warning(
                        f"⚠ {selected_gap.rejected_similar_count} lignende tur(e) er afvist tidligere. "
                        f"Top reasons:\n" +
                        "\n".join(f"  • {r}" for r in selected_gap.rejected_similar_reasons[:5])
                    )
            with c2:
                if st.button("✓ Godkend som lead", type="primary", use_container_width=True):
                    try:
                        from topas_scraper._pg_conn import connect as pg_connect
                        from datetime import datetime, timezone
                        conn = pg_connect()
                        conn.execute("""
                            INSERT INTO n8n_candidates (
                                competitor_domain, topas_tour_code, search_country,
                                tour_name, tour_url, next_departure, tour_category,
                                duration_days, match_confidence, notes,
                                has_match, has_guide, has_fixed_departures,
                                searched_at, imported_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            domain,
                            "DISCOVERY",
                            t.country,
                            t.tour_name,
                            t.url,
                            t.next_departure,
                            t.activity,
                            t.duration_days,
                            "medium",
                            f"From Discovery (score {selected_gap.score:.1f}): {selected_gap.gap_reason}",
                            1, 1 if t.has_guide else 0, 1 if t.has_fixed_departures else 0,
                            datetime.now(timezone.utc).isoformat(),
                            datetime.now(timezone.utc).isoformat(),
                        ))
                        conn.commit()
                        st.success(
                            "✓ Tilføjet til Review-kandidater. "
                            "Gå til pages/5 for manuel godkendelse + tildeling af tour_code."
                        )
                    except Exception as exc:
                        st.error(f"Insert fejlede: {type(exc).__name__}: {exc}")


with st.expander("ℹ Hvordan virker Discovery?"):
    st.markdown("""
**5-trins flow** (én konkurrent ad gangen):

1. **URL-discovery** — sitemap.xml først (gratis), Firecrawl `/map` som fallback hvis sitemap er thin.
2. **Per-tur scrape + klassificering** — Firecrawl JSON-extract → Claude classifier vurderer hvert tour på 8 felter (tour_name, country, activity, duration, has_guide, has_fixed_departures, from_price, icp_match).
3. **ICP-filter** — kun ture der opfylder:
   - `has_guide = True` (dansk-talende rejseleder)
   - `has_fixed_departures = True` (publicerede datoer + priser)
   - `activity ∈ {Vandring, Trekking, Cykling, Sejlads og vandring, Højrute, ...}`
   - `duration_days ∈ [4, 25]`
4. **Gap-analyse** — sammenlign med 49 aktive Topas-ture på `(land × aktivitet × varighed-band)`. Hvis ingen Topas-tur matcher → GAP.
5. **Scoring** = `min(departures_12mo, 12) × 0.83 × country_priority − rejection_malus`
   - `country_priority`: Grønland ×1.5, Italien/Spanien ×1.3, Nepal/Vietnam/Portugal/Frankrig/Kroatien ×1.2
   - `rejection_malus`: −1 per lignende tur afvist i `review_decisions` (185 historiske afvisninger som negativ-eksempler)

Resultat-tabel er sorteret efter score, faldende. Højere score = mere markedsvalideret + mere strategisk værdifuld at undersøge.
""")
