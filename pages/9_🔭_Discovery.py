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

import dataclasses
import json
import re
import threading
import time
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Discovery · Topas", page_icon="🔭", layout="wide")

# Password-gate
from topas_scraper._auth import require_auth  # noqa: E402
require_auth()


# ---------------------------------------------------------------------------
# Persistens — gem til Supabase (overlever Streamlit Cloud rebuilds)
# ---------------------------------------------------------------------------
#
# Tempdir nulstilles ved hver container-rebuild (= hver git push). Vi laver
# i stedet en discovery_runs-tabel i Supabase med JSONB. UPSERT per
# operator_slug, saa vi har altid den nyeste koersel per konkurrent.
#
# Dataclasses (GapResult, CompetitorTour) konverteres til/fra dict via
# dataclasses.asdict() ved save, og rehydreres ved load. Det betyder ogsaa
# at vi er robuste paa tvars af kode-aendringer (default values paa nye
# felter loader gamle runs uden krak).

def _safe_slug(name: str) -> str:
    """Lav operator-navn om til SQL-safe slug."""
    s = re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")
    return s or "unknown"


def _serialize_payload(payload: dict) -> dict:
    """Konvertér payload (med dataclasses i gaps) til pure JSON-safe dict."""
    def _convert(obj):
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        if isinstance(obj, (list, tuple)):
            return [_convert(x) for x in obj]
        if isinstance(obj, dict):
            return {k: _convert(v) for k, v in obj.items()}
        return obj
    return _convert(payload)


def _rehydrate_payload(data: dict) -> dict:
    """Konvertér loaded dict tilbage til payload med GapResult/CompetitorTour
    dataclasses i gaps-listen. UI-koden forventer attribut-access (g.tour.url
    etc.) saa vi reconstruerer dataclasses fra de gemte dicts.

    Defensiv: hvis loaded data er for gammel og mangler nye felter, bruges
    default-values fra dataclasses.
    """
    from topas_scraper.competitor_discovery import GapResult, CompetitorTour

    result = data.get("result", {})
    raw_gaps = result.get("gaps", [])
    rehydrated_gaps = []
    for g in raw_gaps:
        if not isinstance(g, dict):
            continue
        tour_d = g.get("tour", {}) or {}
        try:
            tour = CompetitorTour(
                operator=tour_d.get("operator", ""),
                url=tour_d.get("url", ""),
                slug=tour_d.get("slug", ""),
                tour_name=tour_d.get("tour_name", ""),
                country=tour_d.get("country"),
                activity=tour_d.get("activity"),
                duration_days=tour_d.get("duration_days"),
                has_guide=tour_d.get("has_guide", False),
                has_fixed_departures=tour_d.get("has_fixed_departures", False),
                next_departure=tour_d.get("next_departure"),
                departure_count_next_12mo=tour_d.get("departure_count_next_12mo", 0),
                from_price_dkk=tour_d.get("from_price_dkk"),
                icp_match=tour_d.get("icp_match", False),
                classifier_notes=tour_d.get("classifier_notes", ""),
            )
            rehydrated_gaps.append(GapResult(
                tour=tour,
                gap_reason=g.get("gap_reason", ""),
                score=float(g.get("score", 0)),
                rejected_similar_count=g.get("rejected_similar_count", 0),
                rejected_similar_reasons=g.get("rejected_similar_reasons", []) or [],
                near_gap_warning=g.get("near_gap_warning", ""),
            ))
        except (TypeError, ValueError):
            continue
    result["gaps"] = rehydrated_gaps
    data["result"] = result
    return data


def _save_run(operator: str, domain: str | None, payload: dict) -> None:
    """UPSERT en discovery-koersel til Supabase discovery_runs-tabellen."""
    try:
        from topas_scraper._pg_conn import connect as pg_connect
        conn = pg_connect()
        slug = _safe_slug(operator)
        json_payload = json.dumps(_serialize_payload(payload), default=str)
        conn.execute("""
            INSERT INTO discovery_runs (operator_slug, operator, domain, result_json, completed_at)
            VALUES (?, ?, ?, ?, NOW())
            ON CONFLICT (operator_slug) DO UPDATE SET
                operator = EXCLUDED.operator,
                domain = EXCLUDED.domain,
                result_json = EXCLUDED.result_json,
                completed_at = EXCLUDED.completed_at
        """, (slug, operator, domain, json_payload))
        conn.commit()
    except Exception:
        # Bevidst silent — persistence er nice-to-have, ikke critical path
        pass


def _list_saved_runs() -> list[tuple[str, datetime, str]]:
    """Returnér liste af (operator_slug, completed_at, operator_display_name)
    sorteret nyeste foerst."""
    try:
        from topas_scraper._pg_conn import connect as pg_connect
        conn = pg_connect()
        rows = conn.execute("""
            SELECT operator_slug, operator, completed_at
            FROM discovery_runs
            ORDER BY completed_at DESC
        """).fetchall()
        out: list[tuple[str, datetime, str]] = []
        for r in rows:
            d = dict(r)
            out.append((d["operator_slug"], d["completed_at"], d["operator"]))
        return out
    except Exception:
        return []


def _load_run_by_slug(operator_slug: str) -> dict | None:
    """Indlaes en gemt koersel via operator_slug. Returnerer rehydreret
    payload (med dataclasses), eller None hvis ikke fundet."""
    try:
        from topas_scraper._pg_conn import connect as pg_connect
        conn = pg_connect()
        rows = conn.execute("""
            SELECT operator, domain, result_json, completed_at
            FROM discovery_runs
            WHERE operator_slug = ?
        """, (operator_slug,)).fetchall()
        if not rows:
            return None
        d = dict(rows[0])
        raw = d["result_json"]
        if isinstance(raw, str):
            raw = json.loads(raw)
        payload = {
            "operator": d["operator"],
            "domain": d.get("domain"),
            "result": raw.get("result", {}) if isinstance(raw, dict) else {},
            "completed_at": d["completed_at"].strftime("%H:%M:%S") if d.get("completed_at") else "",
        }
        return _rehydrate_payload(payload)
    except Exception:
        return None


def _age_str_from_dt(dt: datetime) -> str:
    """Format alder paa et timestamp som '5 min siden' / '2 timer siden'."""
    if dt is None:
        return "ukendt"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    minutes = delta.total_seconds() / 60
    if minutes < 60:
        return f"{int(minutes)} min siden"
    if minutes < 24 * 60:
        return f"{minutes / 60:.1f} timer siden"
    return f"{minutes / (24 * 60):.0f} dage siden"


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
        # NB: /sitemap.xml indeholder URLs fra et andet domaene (skiinstruktor.no
        # som er soester-projekt). Ruby's egne URLs ligger i ruby_DK_sitemap.xml.
        "sitemap": "https://ruby-rejser.dk/ruby_DK_sitemap.xml",
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
        "homepage": "https://www.vagabondtours.dk",
        # Vagabond migrerede fra ASP.NET til WordPress. Sitemap_index.xml
        # peger paa tours-sitemap.xml (181 ture) + 7 andre sub-sitemaps.
        "sitemap": "https://www.vagabondtours.dk/sitemap_index.xml",
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
        key="op_choice_key",  # giver os mulighed for at saette vaerdien programmatisk
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
        min_value=5, max_value=2000, value=200, step=50,
        help="Cap på antal URLs at scrape (~1 Firecrawl credit + 1 Claude-kald pr. URL). "
             "Du har ~80k credits tilbage — sæt 500-1000 for fuld dækning af store sites "
             "(Ruby 424 ture, Albatros 600+, Jysk 200+ efter filter)."
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

# Sync session_state med dropdown: hvis det aktuelt loadede resultat ikke
# matcher den valgte konkurrent, load DEN konkurrents gemte run (eller
# clear hvis ingen findes). Saa undgaar vi at Viktors-resultatet bliver
# staende naar user vaelger Jysk i dropdownen.
current_op_slug = _safe_slug(op_name)
_loaded = st.session_state.get("discovery_result")
_loaded_slug = _safe_slug(_loaded.get("operator", "")) if _loaded else None

if _loaded_slug != current_op_slug:
    restored = _load_run_by_slug(current_op_slug)
    st.session_state["discovery_result"] = restored

# Vis banner med info om alder paa det loadede resultat
_loaded = st.session_state.get("discovery_result")
if _loaded:
    _saved_now = _list_saved_runs()
    _completed_at = next(
        (dt for slug, dt, _ in _saved_now if slug == current_op_slug),
        None,
    )
    if _completed_at is not None:
        st.info(
            f"📂 Viser sidste kørsel for **{_loaded.get('operator', op_name)}** "
            f"({_age_str_from_dt(_completed_at)}). Kør discovery igen for fresh data."
        )

# Hvis vi har flere gemte kørsler — vis dem som hurtig-skift
saved_runs = _list_saved_runs()
if len(saved_runs) > 1:
    with st.expander(f"📂 {len(saved_runs)} gemte kørsler — skift mellem konkurrenter"):
        cols = st.columns(min(len(saved_runs), 4))
        for i, (slug, dt, display) in enumerate(saved_runs):
            with cols[i % len(cols)]:
                short_age = _age_str_from_dt(dt).replace(" siden", "")
                btn_label = f"{display}\n({short_age})"
                if st.button(btn_label, key=f"load_{slug}", use_container_width=True):
                    restored = _load_run_by_slug(slug)
                    if restored:
                        st.session_state["discovery_result"] = restored
                        # Sync dropdown ogsaa — find display-navn i payload og
                        # set selectbox-key saa dropdownen matcher
                        display_name = restored.get("operator")
                        if display_name in COMPETITORS:
                            st.session_state["op_choice_key"] = display_name
                        st.rerun()

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
                domain=op_meta.get("domain"),
            )
            payload = {
                "operator": op_name,
                "domain": op_meta["domain"],
                "result": result,
                "completed_at": time.strftime("%H:%M:%S"),
            }
            st.session_state["discovery_result"] = payload
            # Persistér til Supabase så Streamlit Cloud rebuild ikke smider
            # resultatet væk (tempdir nulstilles ved hver git push)
            _save_run(op_name, op_meta.get("domain"), payload)
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

    # Diagnostik: hvorfor blev ture afvist af ICP-filter?
    breakdown = result["stats"].get("rejection_breakdown") or {}
    examples = result["stats"].get("rejected_examples") or []
    if breakdown:
        rejected_total = result["tours_classified"] - result["icp_passing"]
        with st.expander(
            f"🔍 ICP-afvisninger: {rejected_total} ture afvist — se hvorfor",
            expanded=(result["icp_passing"] == 0),
        ):
            st.caption(
                "Hvis ALLE ture afvises pga. samme grund, er classifier-prompten "
                "muligvis for streng eller URL-pattern fanger non-tour pages. "
                "Tjek eksemplerne nedenfor."
            )
            top = sorted(breakdown.items(), key=lambda kv: -kv[1])
            st.markdown("**Afvisnings-grunde:**")
            for reason, count in top:
                pct = (count / rejected_total * 100) if rejected_total else 0
                st.markdown(f"- `{reason}` — **{count}** ture ({pct:.0f}%)")

            if examples:
                st.markdown("**Eksempler (8 første afviste ture):**")
                for ex in examples:
                    st.markdown(
                        f"- [{ex['tour_name'][:60]}]({ex['url']}) — "
                        f"`{ex['country']}` / `{ex['activity']}` / "
                        f"`{ex['duration_days']}d` / "
                        f"guide={ex['has_guide']} fixed_dep={ex['has_fixed_departures']}"
                    )
                    if ex.get("classifier_notes"):
                        st.caption(f"  └ Claude: {ex['classifier_notes']}")
                    st.markdown(f"  └ Grunde: `{', '.join(ex['reasons'])}`")

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
            # Vis ⚠ ikon foran tur-navn hvis Topas har lignende tur i anden duration
            "Tur": (("⚠ " if getattr(g, "near_gap_warning", "") else "") + g.tour.tour_name)[:65],
            "Link": g.tour.url,  # renderes som klikbar "🔗 Åbn" via column_config
            "Land": g.tour.country or "?",
            "Aktivitet": g.tour.activity or "?",
            "Dage": g.tour.duration_days or "?",
            "Afgange (12mdr)": g.tour.departure_count_next_12mo,
            "Næste afgang": g.tour.next_departure or "—",
            "Fra-pris (kr)": f"{g.tour.from_price_dkk:,}".replace(",", ".") if g.tour.from_price_dkk else "—",
            "Gap-grund": g.gap_reason,
            "Lignende afvist": g.rejected_similar_count,
        }
        for g in gaps
    ])

    # Tael near-gaps og oplys brugeren om ⚠-ikonet
    near_gap_count = sum(1 for g in gaps if getattr(g, "near_gap_warning", ""))
    if near_gap_count:
        st.caption(
            f"⚠ = {near_gap_count} af gap-turne har en lignende Topas-tur "
            "i en anden varighed (samme destination, anden duration-band). "
            "Score er reduceret med 40% for disse. Se detalje-panelet nederst for varianten."
        )

    def _color_score(v):
        """Heatmap-tærskler (score 0-15+):
           >=10 → kraftig grøn (bestseller / valideret + Topas-fokus-land)
           >=6  → mellem grøn (etableret efterspørgsel)
           >=3  → lys grøn (svag-til-god efterspørgsel)
           <2   → rød (test-tur eller niche — lavt signal)
        """
        if pd.isna(v):
            return ""
        if v >= 10:
            return "background-color: rgba(46, 139, 87, 0.55); font-weight:700;"
        if v >= 6:
            return "background-color: rgba(46, 139, 87, 0.35);"
        if v >= 3:
            return "background-color: rgba(46, 139, 87, 0.15);"
        if v < 2:
            return "background-color: rgba(192, 57, 43, 0.35);"
        return ""

    styled = df.style.map(_color_score, subset=["Score"])
    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=600,
        column_config={
            "Link": st.column_config.LinkColumn(
                "Link",
                display_text="🔗 Åbn",
                width="small",
                help="Åbn turen på konkurrentens website",
            ),
            "Score": st.column_config.NumberColumn(
                "Score",
                format="%.1f",
                width="small",
            ),
        },
    )

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
                # Near-gap warning: Topas har lignende destination i anden duration
                near_gap = getattr(selected_gap, "near_gap_warning", "")
                if near_gap:
                    st.info(f"💡 **Lignende Topas-tur findes:** {near_gap}")
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
5. **Scoring** = `departure_validation × country_priority − rejection_malus`
   - `departure_validation` (NON-LINEAR markeds-validering):
     - 0 afgange → 0 (ingen markedssignal)
     - 1 afgang → 1 (test-tur / niche — svagt signal)
     - 2-3 afgange → 3 (svag efterspørgsel)
     - 4-6 afgange → 6 (god efterspørgsel — etableret tur)
     - 7-11 afgange → 8 (stærk efterspørgsel — populær)
     - 12+ afgange → 10 (bestseller)
   - `country_priority`: Grønland ×1.5, Italien/Spanien ×1.3, Nepal/Vietnam/Portugal/Frankrig/Kroatien ×1.2
   - `rejection_malus`: blød −0.3 per lignende KONTENT-blok-afvisning (cap −1.5). Screening-noise filtreres væk.

   Max score: ~15 (bestseller i Grønland). Typisk høj score: 8-12.

Resultat-tabel er sorteret efter score, faldende. Højere score = mere markedsvalideret + mere strategisk værdifuld at undersøge.
""")
