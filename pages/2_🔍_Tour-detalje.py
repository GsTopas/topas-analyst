"""
Tour-detalje — d\u00e6kket pr. Topas-tour med konkurrent-picker, per-departure
sammenligning, og **live scrape**-knap.

Den her side er der hvor det egentlige arbejde sker. Hovedet p\u00e5 agenturet \u00e5bner
en tour, ser n\u00f8gletal, skifter mellem konkurrenter, og kan trigger en live
scrape n\u00e5r han vil have friske data.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

# Reuse the existing scraper code — Streamlit just orchestrates UI.
from topas_scraper.runner import run_scrape_for_tour, run_scrape_all

st.set_page_config(page_title="Tour-detalje · Topas", page_icon="🔍", layout="wide")

# Password-gate — stopper page-rendering indtil korrekt adgangskode.
from topas_scraper._auth import require_auth  # noqa: E402
require_auth()


# Smallere sidebar så tabellen får mere plads
st.markdown(
    """
    <style>
        [data-testid="stSidebar"] { min-width: 180px !important; max-width: 200px !important; }
        [data-testid="stSidebar"] > div:first-child { width: 200px !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown("# 🔍 Tour-detalje")

# ---------------------------------------------------------------------------
# Load data — generér dashboard-payload fra Supabase ved hver page-load.
#
# Tidligere læste vi fra committed data/dashboard.json som var lavet af
# scraper'en lokalt. Med Supabase som single source of truth bygger vi i
# stedet payload'en direkte fra DB hver gang. @st.cache_data caches i 60s
# så side-navigation ikke forsinkes; den bliver invalidatet automatisk
# efter en scrape (eller manuelt via R-tasten).
# ---------------------------------------------------------------------------
import tempfile

JSON_PATH = Path(tempfile.gettempdir()) / "topas_dashboard.json"


@st.cache_data(ttl=600)
def load_data() -> Optional[dict]:
    """Generér dashboard-payload fra Supabase. Cached 10 min for hurtig nav."""
    try:
        from topas_scraper.export import export as _export
        _export(output=JSON_PATH)
        return json.loads(JSON_PATH.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        # Vis fejlen eksplicit i UI'et — den gamle silent-fallback skjulte
        # at appen læste forældede data fra committed dashboard.json.
        st.error(
            f"⚠ Export fra Supabase fejlede: `{type(exc).__name__}: {exc}`\n\n"
            "Falder tilbage til committed `data/dashboard.json` som er en gammel "
            "snapshot. Nye ture (fx FRCL) mangler. Send fejl-tracen til debugging."
        )
        import traceback  # noqa: PLC0415
        with st.expander("Stack trace (debug)", expanded=False):
            st.code(traceback.format_exc())

        # Fall back til committed dashboard.json så app ikke crasher
        fallback = Path("data/dashboard.json")
        if fallback.exists():
            try:
                return json.loads(fallback.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return None
        return None


data = load_data()
if data is None:
    st.warning(
        "Ingen scraped data. Kør `python -m topas_scraper.cli scrape` lokalt, eller brug "
        "**Live scrape**-knappen herunder."
    )

scraped_tours = (data or {}).get("tours", [])
competitors_data = (data or {}).get("competitors", [])

# Build a lookup: tour_code -> scraped tour dict (so we can quickly check
# whether a given catalog tour has any scraped data attached).
scraped_by_code: dict[str, dict] = {t["code"]: t for t in scraped_tours}

# ---------------------------------------------------------------------------
# Tour picker — reads from topas_catalog (all 49 Topas tours).
# A single source of truth: the catalog. Tours without scraped data are still
# selectable (so the user can scan them via n8n) but show a different UI.
# ---------------------------------------------------------------------------
from topas_scraper.db import connect, fetch_topas_catalog
from topas_scraper import catalog_db as _catdb


@st.cache_data(ttl=10)
def _category_by_url() -> dict:
    """Return {tour_url: category-emoji-label} from approved_competitor_targets.
    Cached briefly so we don't hit DB on every row."""
    try:
        c = _catdb.connect()
        approved = _catdb.list_approved_targets(c)
        c.close()
    except Exception:
        return {}
    label = {
        "vandre": "🥾 Vandre",
        "cykel": "🚴 Cykel",
        "kultur": "🏛️ Kultur",
        "kombineret": "🔀 Kombineret",
        "andet": "❓ Andet",
    }
    out = {}
    for a in approved:
        cat = (a.get("tour_category") or "").lower()
        if a.get("tour_url"):
            out[a["tour_url"]] = label.get(cat, "—")
    return out


_CATEGORY_LOOKUP = _category_by_url()


# Sitemap-URLs per konkurrent — hentes af _fetch_sitemap_urls() inden screening
# for at få 100% dækning af tour-URLs (uafhængigt af Google's index-ranking).
# Tilføj nye operatører her efterhånden som vi bekræfter de har sitemaps.
_SITEMAP_URLS = {
    "albatros.dk": "https://www.albatros.dk/sitemap.xml",
    "gjoa.dk": "https://gjoa.dk/sitemap.xml",
    "viktorsfarmor.dk": "https://www.viktorsfarmor.dk/sitemap.xml",
    "stjernegaard-rejser.dk": "https://www.stjernegaard-rejser.dk/sitemap.xml",
    "smilrejser.dk": "https://smilrejser.dk/sitemap.xml",
    "ruby-rejser.dk": "https://ruby-rejser.dk/sitemap.xml",
    # Tilføj flere efterhånden som vi bekræfter de har sitemap. Funktionen
    # _fetch_sitemap_urls er defensiv — returnerer [] hvis sitemap ikke
    # findes, så det er sikkert at tilføje speculativt.
}


@st.cache_data(ttl=3600)  # cache 1 time — sitemaps ændres sjældent
def _fetch_sitemap_urls(domain: str, country: str, keyword: str = "") -> list[str]:
    """Hent operatørens sitemap.xml og filtrer URLs på land + valgfri keyword.

    Returns max 30 matches. Returnerer tom liste hvis sitemap ikke findes
    eller fetch fejler.
    """
    sitemap_url = _SITEMAP_URLS.get(domain)
    if not sitemap_url:
        return []

    import requests as _req  # noqa: PLC0415
    from xml.etree import ElementTree as _ET  # noqa: PLC0415

    try:
        resp = _req.get(sitemap_url, timeout=10)
        if resp.status_code != 200:
            return []

        # Parse XML — håndter både simple sitemaps og sitemap-indexes
        root = _ET.fromstring(resp.content)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        # Hvis det er et sitemap-index (peger på sub-sitemaps), følg dem
        sub_sitemaps = [s.text for s in root.findall(".//sm:sitemap/sm:loc", ns)]
        all_urls: list[str] = []
        if sub_sitemaps:
            # Hent op til 5 sub-sitemaps (de fleste sites har 1-3 relevante)
            for sub in sub_sitemaps[:5]:
                try:
                    sub_resp = _req.get(sub, timeout=10)
                    if sub_resp.status_code == 200:
                        sub_root = _ET.fromstring(sub_resp.content)
                        all_urls.extend(u.text for u in sub_root.findall(".//sm:url/sm:loc", ns))
                except Exception:
                    continue
        else:
            # Direkte sitemap (ikke index)
            all_urls = [u.text for u in root.findall(".//sm:url/sm:loc", ns)]

        # Filter på land + valgfri keyword.
        # Keyword-syntax: AND splitter krav, OR splitter alternativer
        # indenfor hvert krav. Precedence: OR > AND.
        # Eksempler:
        #   'Apulien'                          → 1 krav: [['apulien']]
        #   'Apulien AND Cykling'              → 2 krav: [['apulien'], ['cykling']]
        #   'Apulien AND Cykling OR Cykelferie' → [['apulien'], ['cykling','cykelferie']]
        #   'Cykling OR Cykelferie AND Apulien' → [['cykling','cykelferie'], ['apulien']]
        country_lc = country.lower()
        and_groups: list[list[str]] = []
        if keyword:
            for grp in keyword.split(" AND "):
                or_terms = [t.strip().lower() for t in grp.split(" OR ") if t.strip()]
                if or_terms:
                    and_groups.append(or_terms)

        filtered = []
        for url in all_urls:
            if not url:
                continue
            url_lc = url.lower()
            if country_lc not in url_lc:
                continue
            # Hver AND-gruppe skal have mindst ét OR-term der matcher
            if and_groups:
                if not all(any(kw in url_lc for kw in grp) for grp in and_groups):
                    continue
            filtered.append(url)

        return filtered[:30]  # cap for at undgå at scrape 100+ URLs
    except Exception:
        return []


# Canonical operator names — så vi viser samme navn uanset om data kommer
# fra ny pipeline (domæne-baseret) eller gammel (parser-konstrueret).
# Mappen kobler både domæne og kendte alias-skrivemåder til kanonisk navn.
_OPERATOR_CANONICAL = {
    # Smilrejser
    "smilrejser.dk": "Smilrejser", "smilrejser": "Smilrejser",
    # Jysk Rejsebureau
    "jysk-rejsebureau.dk": "Jysk Rejsebureau", "jyskrejsebureau": "Jysk Rejsebureau",
    # Viktors Farmor
    "viktorsfarmor.dk": "Viktors Farmor", "viktorsfarmor": "Viktors Farmor",
    # Ruby Rejser
    "ruby-rejser.dk": "Ruby Rejser", "rubyrejser": "Ruby Rejser",
    # Stjernegaard Rejser
    "stjernegaard-rejser.dk": "Stjernegaard Rejser", "stjernegaardrejser": "Stjernegaard Rejser",
    # Albatros Travel
    "albatros.dk": "Albatros Travel", "albatros": "Albatros Travel", "albatrostravel": "Albatros Travel",
    # Kipling Travel
    "kiplingtravel.dk": "Kipling Travel", "kiplingtravel": "Kipling Travel", "kipling": "Kipling Travel",
    # Fyrholt Rejser
    "fyrholtrejser.dk": "Fyrholt Rejser", "fyrholtrejser": "Fyrholt Rejser", "fyrholt": "Fyrholt Rejser",
    # Vagabond Tours
    "vagabondtours.dk": "Vagabond Tours", "vagabondtours": "Vagabond Tours", "vagabond": "Vagabond Tours",
    # Gjøa
    "gjoa.dk": "Gjøa", "gjoa": "Gjøa",
    # Topas
    "topas.dk": "Topas", "topas": "Topas",
}


def _canonical_op(name: str | None) -> str:
    """Return canonical operator name regardless of input casing/spacing/domain.
    Bruges i UI for at undgå at vise dublerede operator-navne."""
    if not name:
        return "—"
    key = name.lower().replace(" ", "").replace("-", "").replace(".", "")
    # Try direct match first (with the key-normalisation)
    for k, v in _OPERATOR_CANONICAL.items():
        kk = k.lower().replace(" ", "").replace("-", "").replace(".", "")
        if kk == key:
            return v
    # Fallback: return as-is
    return name


def _auto_category_label(tour_name: str | None, tour_url: str | None = None) -> str:
    """Best-effort categorization based on name + URL keywords.

    For Topas-tours defaultes til 'Vandre' når intet specifikt signal —
    fordi Topas er en vandre-operatør (~95%+ af deres ture er vandring).
    For andre operatører returneres '—' når intet signal matches.
    """
    import re as _re
    name = (tour_name or "").lower()
    url = (tour_url or "").lower()
    text = f"{name} {url}"
    is_topas = "topas.dk" in url

    # Cykel — eksplicitte cykel-stems
    has_cykel = bool(_re.search(
        r"cykel|cykling|cykle|cykeltur|bike|bicycl|cycling",
        text,
    ))

    # Vandre — eksplicitte vandre-stems + kendte aktive destinationer
    has_vandre = bool(_re.search(
        r"vandr|hike|trek|trail|levada|alper|tinder|peaks|"
        r"bjerge|bjerg-|aktiv ferie|aktivferie|gåtur|"
        r"trekking|fjeld|kløft|naturperle",
        text,
    ))

    # Kultur — KUN strong cultural signals; ingen marketing-prosa der
    # ofte optræder i vandretur-navne (fjernet: perle, oase, smage, nyde, charm)
    has_kultur = bool(_re.search(
        r"kultur|rundrejse|all.?inclusive|all.?incl|tidsrejse|"
        r"vingård|vinsmagning|vinrejse|vintur|"
        r"kloster|klosterophold|byrundrejse|"
        r"kulinarisk|gastronom|madkultur",
        text,
    ))

    if has_cykel and has_vandre:
        return "🔀 Kombineret"
    if has_cykel:
        return "🚴 Cykel"
    if has_vandre and has_kultur:
        return "🔀 Kombineret"
    if has_vandre:
        return "🥾 Vandre"
    if has_kultur:
        return "🏛️ Kultur"
    # Fallback: Topas defaults til vandre (deres standardprofil)
    if is_topas:
        return "🥾 Vandre"
    return "—"

try:
    _conn = connect()
    _catalog_rows = fetch_topas_catalog(_conn)
    catalog_tours = [dict(row) for row in _catalog_rows]
except Exception as e:
    st.error(f"Kunne ikke læse Topas-katalog: {e}")
    catalog_tours = []

if not catalog_tours:
    st.warning(
        "Topas-kataloget er tomt. Gå til **📋 Topas-katalog** og klik **Refresh** "
        "for at populere det med alle 49 Topas-ture."
    )
    st.stop()

# Sort: ture med scraped data først (så du ser dem først), derefter resten
def _sort_key(t: dict) -> tuple:
    has_data = 0 if t.get("tour_code") in scraped_by_code else 1
    country = (t.get("country") or "ÅÅÅ").lower()
    name = (t.get("tour_name") or "").lower()
    return (has_data, country, name)

catalog_tours.sort(key=_sort_key)

# Build set of tour-codes with approved competitors — used for dropdown status
@st.cache_data(ttl=10)
def _codes_with_approved_competitors() -> set:
    try:
        c = _catdb.connect()
        approved = _catdb.list_approved_targets(c)
        c.close()
    except Exception:
        return set()
    return {a["topas_tour_code"] for a in approved if a.get("topas_tour_code")}

_CODES_WITH_COMPS = _codes_with_approved_competitors()


# Build dropdown options: tour_code → friendly label
def _format_tour_option(code: Optional[str]) -> str:
    """Format a tour code for display in the dropdown.
    Status emoji:
      🟢 = Topas scrapet OG har godkendte konkurrenter (klar til pris-sammenligning)
      🟡 = Topas scrapet men INGEN godkendte konkurrenter (skal screenes først)
      ⚪ = Hverken Topas eller konkurrenter scrapet endnu
    """
    tour = next((t for t in catalog_tours if t.get("tour_code") == code), None)
    if not tour:
        return code or "(ingen kode)"

    in_scraped = code in scraped_by_code
    has_comps = code in _CODES_WITH_COMPS
    if in_scraped and has_comps:
        status = "🟢"
    elif in_scraped or has_comps:
        status = "🟡"
    else:
        status = "⚪"
    name = tour.get("tour_name") or "(ingen navn)"
    country = tour.get("country") or "?"
    return f"{status}  {code or '(?)'} — {name}  ·  {country}"

# Use tour_code as the option value (some tours may not have one — those get a
# synthetic ID based on URL slug)
def _option_id(t: dict) -> str:
    return t.get("tour_code") or f"_no_code_{t.get('url', '')[-30:]}"

option_ids = [_option_id(t) for t in catalog_tours]

# Restore preselection from session
default_code = st.session_state.get("selected_tour_code", option_ids[0])
default_idx = option_ids.index(default_code) if default_code in option_ids else 0

selected_option_id = st.selectbox(
    "Vælg tour",
    option_ids,
    index=default_idx,
    format_func=_format_tour_option,
    help="🟢 = klar (Topas + konkurrenter) · 🟡 = mangler konkurrenter eller scrape · ⚪ = intet scrapet endnu",
)

# Resolve the selected catalog tour
selected_catalog_tour = next(
    (t for t in catalog_tours if _option_id(t) == selected_option_id),
    None,
)
if not selected_catalog_tour:
    st.error("Kunne ikke finde den valgte tur i kataloget.")
    st.stop()

selected_code = selected_catalog_tour.get("tour_code")
st.session_state["selected_tour_code"] = selected_option_id

# Find scraped data for this tour, if any
selected_tour = scraped_by_code.get(selected_code) if selected_code else None

# ---------------------------------------------------------------------------
# Live scrape button — only shown if this tour has TARGETS configured
# (otherwise scraping does nothing). For tours without TARGETS, the n8n
# screening section below is the path forward.
# ---------------------------------------------------------------------------
try:
    from topas_scraper.config import load_active_targets as _load_targets
    _live_targets = _load_targets()
    ALL_CONFIGURED_CODES = sorted({t.competes_with for t in _live_targets if t.competes_with})
    TOTAL_CONFIGURED_URLS = len(_live_targets)
except Exception:
    ALL_CONFIGURED_CODES = []
    TOTAL_CONFIGURED_URLS = 0

is_configured = selected_code in ALL_CONFIGURED_CODES

col_scrape_one, col_scrape_meta = st.columns([1, 3])

with col_scrape_one:
    scrape_one_clicked = st.button(
        "🔄 Hent konkurrenternes afgange + detaljer" if is_configured
        else "🔄 Ingen konkurrenter tilknyttet",
        use_container_width=True,
        type="primary",
        disabled=not is_configured,
        help=(
            f"Henter Topas + alle godkendte konkurrenter for {selected_code} "
            "via operatør-specifikke parsers. Tager ~30-60 sekunder."
            if is_configured else
            f"Tour {selected_code} har ingen godkendte konkurrenter endnu. "
            "Brug n8n-screeningen nedenfor for at finde og godkende konkurrenter."
        ),
    )

scrape_all_clicked = False  # legacy "scrape all" removed — keeping flag for downstream code

with col_scrape_meta:
    if data and selected_tour:
        ts_raw = data.get("snapshotTakenAt", "")
        try:
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            ts_pretty = ts.strftime("%d. %b %Y · kl. %H:%M")
        except (ValueError, TypeError):
            ts_pretty = ts_raw
        st.caption(f"Sidste snapshot: **{ts_pretty}** · run `{data.get('snapshotRunId', '')[:8]}`")

# Determine what to scrape
scrape_clicked = scrape_one_clicked or scrape_all_clicked
scrape_mode = "all" if scrape_all_clicked else "single"

# Run the scrape if either button clicked
if scrape_clicked:
    # Check for API key — in Streamlit Cloud this comes from secrets, locally from .env
    api_key = os.getenv("FIRECRAWL_API_KEY") or st.secrets.get("FIRECRAWL_API_KEY", "")
    if not api_key:
        st.error(
            "FIRECRAWL_API_KEY mangler. På Streamlit Cloud: tilføj i appens Secrets. "
            "Lokalt: kopier `.env.example` til `.env` og indsæt nøglen."
        )
        st.stop()

    # Make sure runner can find the key
    os.environ["FIRECRAWL_API_KEY"] = api_key

    if scrape_mode == "all":
        status_label = f"Scraper alle {TOTAL_CONFIGURED_URLS} URLs..."
    else:
        status_label = f"Scraper {selected_code}..."

    with st.status(status_label, expanded=True) as status:
        log_lines: list[str] = []

        def emit(msg: str) -> None:
            log_lines.append(msg)
            status.write(msg)

        try:
            if scrape_mode == "all":
                run_id, success, total = run_scrape_all(on_progress=emit)
                done_label = f"✓ Alle tours scrapet ({success}/{total})"
            else:
                run_id, success, total = run_scrape_for_tour(selected_code, on_progress=emit)
                done_label = f"✓ {selected_code} scrapet ({success}/{total})"

            if success == total:
                status.update(label=done_label, state="complete")
                st.success(f"Run `{run_id[:8]}` færdig — {success}/{total} URLs succeeded.")
                # Invalider load_data()-cache så page rerun læser frisk fra Supabase
                # i stedet for den 10-min cachede payload fra før scrape.
                load_data.clear()
                st.rerun()
            else:
                status.update(label=f"⚠ Delvis succes ({success}/{total})", state="error")
                st.warning(
                    f"Kun {success}/{total} URLs lykkedes. Tjek logs ovenfor og kør igen hvis "
                    "nødvendigt."
                )
        except (RuntimeError, ValueError) as e:
            status.update(label=f"✗ Scrape fejlede: {e}", state="error")
            st.error(str(e))
        except Exception as e:
            status.update(label=f"✗ Uventet fejl: {e}", state="error")
            st.exception(e)

# ---------------------------------------------------------------------------
# Tour header — always shown, uses catalog data (works for all 49 tours,
# even if they don't have scraped competitor data yet).
# ---------------------------------------------------------------------------
st.divider()

display_name = (
    selected_tour["name"] if selected_tour else selected_catalog_tour["tour_name"]
)
display_country = (
    selected_tour.get("country") if selected_tour
    else selected_catalog_tour.get("country") or "?"
)
display_duration = (
    selected_tour.get("durationDays") if selected_tour
    else selected_catalog_tour.get("duration_days") or "?"
)

st.caption(
    f"**{display_country}** · "
    f"Turkode `{selected_code or '(?)' }` · "
    f"{display_duration} dage Fællesrejse"
)

# Status banner: indicates whether this tour has scraped competitor data
if not selected_tour:
    st.info(
        f"📊 **Ingen konkurrent-data scrapet endnu** for {selected_code}. "
        "Brug screeningen nedenfor for at finde konkurrenter via n8n. "
        "Når kandidater er bekræftet og tilføjet til konfigurationen, "
        "vil pris-sammenligning vises her."
    )
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Konkurrent-screening via n8n
# ---------------------------------------------------------------------------
# Triggers an async webhook to n8n that searches each registered competitor's
# website for a guided group tour matching this Topas tour's country/region.
# Results land in n8n's "Competitor Analysis" data table (separate from
# dashboard.json — this is exploratory discovery, not the tracked-comparison
# data shown below).

# Source of truth for "registered competitors" — pulled from past scrapes in
# snapshots.db. Keep this in sync as you add new operators.
COMPETITOR_DOMAINS = {
    "Albatros Travel": "albatros.dk",
    "Fyrholt Rejser": "fyrholtrejser.dk",
    "Gjøa": "gjoa.dk",
    "Jysk Rejsebureau": "jysk-rejsebureau.dk",
    "Kipling Travel": "kiplingtravel.dk",
    "Ruby Rejser": "ruby-rejser.dk",
    "Smilrejser": "smilrejser.dk",
    "Stjernegaard Rejser": "stjernegaard-rejser.dk",
    "Vagabond Tours": "vagabondtours.dk",
    "Viktors Farmor": "viktorsfarmor.dk",
}

selected_tour_code = selected_code or "(?)"


def _get_n8n_webhook_url() -> str:
    """Resolve webhook URL from env, secrets, or fall back to production URL."""
    env_url = os.getenv("N8N_COMPETITOR_WEBHOOK_URL")
    if env_url:
        return env_url
    try:
        secret_url = st.secrets.get("N8N_COMPETITOR_WEBHOOK_URL", "")
        if secret_url:
            return secret_url
    except Exception:
        pass
    return "https://topas.app.n8n.cloud/webhook/competitor-tour-search"


# Default values for the screening inputs — pulled from scraped data if
# available, otherwise from catalog (works for all 49 tours).
_default_country = (
    selected_tour.get("country", "") if selected_tour
    else (selected_catalog_tour.get("country") or "")
)
_default_topas_name = (
    selected_tour.get("name", "") if selected_tour
    else (selected_catalog_tour.get("tour_name") or "")
)

with st.expander(
    "🔍 Screen konkurrenter for denne tur",
    expanded=not selected_tour,  # Auto-expand for unmapped tours — that's the next step
):
    st.caption(
        "Søger på alle registrerede konkurrenters sites efter rejseleder-ture "
        "til denne destination. Kører i baggrunden via n8n — resultater "
        "lander i 'Competitor Analysis'-tabellen om ~1-3 min."
    )

    sc1, sc2 = st.columns(2)
    with sc1:
        screen_country = st.text_input(
            "Land",
            value=_default_country,
            key=f"screen_country_{selected_tour_code}",
        )
    with sc2:
        screen_region = st.text_input(
            "Region eller aktivitet (valgfri)",
            placeholder="fx 'Madeira', 'Apulien AND Cykling OR Cykelferie'",
            help=(
                "Geografisk region eller aktivitet. Operatorer:\n"
                "• `OR` mellem synonymer/alternativer (samme koncept)\n"
                "• `AND` mellem forskellige krav (alle skal opfyldes)\n"
                "Eksempler:\n"
                "• `Madeira` — én region\n"
                "• `Cykling OR Cykelferie` — enten/eller (synonymer)\n"
                "• `Apulien AND Cykling OR Cykelferie` — Apulien OG en cykel-tur\n"
                "Precedence: OR binder tættere end AND."
            ),
            key=f"screen_region_{selected_tour_code}",
        )

    selected_ops = st.multiselect(
        "Konkurrenter at screene",
        options=list(COMPETITOR_DOMAINS.keys()),
        default=list(COMPETITOR_DOMAINS.keys()),
        key=f"screen_ops_{selected_tour_code}",
    )

    can_send = bool(screen_country and selected_ops)
    if st.button(
        f"🔍 Send screening til n8n ({len(selected_ops)} konkurrenter)",
        type="primary",
        disabled=not can_send,
        key=f"screen_btn_{selected_tour_code}",
    ):
        import requests  # noqa: PLC0415

        # Topas-tour duration sendes som reference — n8n bruger den til at
        # scorere konkurrent-tours: matchende ±15% (min ±2 dage) = high,
        # ellers medium.
        _ref_duration = (
            selected_tour.get("durationDays") if selected_tour
            else selected_catalog_tour.get("duration_days")
        )

        # Hent sitemap-URLs for konkurrenter med kendt sitemap (Albatros har).
        # Disse mergeres med Firecrawl Search i n8n for at få 100% coverage —
        # Firecrawl Search misser ofte tours der rangerer i position 11-20+.
        _sitemap_hints: dict[str, list[str]] = {}
        for op_label in selected_ops:
            domain = COMPETITOR_DOMAINS[op_label]
            sm_urls = _fetch_sitemap_urls(
                domain=domain,
                country=screen_country.strip(),
                keyword=screen_region.strip(),
            )
            if sm_urls:
                _sitemap_hints[domain] = sm_urls

        payload = {
            "competitors": ",".join(COMPETITOR_DOMAINS[o] for o in selected_ops),
            "country": screen_country.strip(),
            "region": screen_region.strip(),
            "topasTourCode": selected_tour_code,
            "topasTourName": _default_topas_name,
            "topasDurationDays": _ref_duration if _ref_duration else None,
            "sitemapHints": _sitemap_hints,  # {domain: [urls]} for ops with sitemaps
        }

        try:
            r = requests.post(_get_n8n_webhook_url(), json=payload, timeout=10)
            if r.status_code in (200, 202):
                # Capture run state for progress polling below.
                # We use this to know which rows in n8n's data table
                # belong to THIS screening (vs older screenings).
                st.session_state["screening_in_flight"] = {
                    "started_iso": datetime.utcnow().isoformat(),
                    "tour_code": selected_tour_code,
                    "competitor_domains": [
                        COMPETITOR_DOMAINS[o] for o in selected_ops
                    ],
                    "competitor_labels": list(selected_ops),
                    "expected_count": len(selected_ops),
                    "completed": False,
                    "done_count": 0,
                    "payload": payload,
                }
                st.rerun()
            else:
                st.error(
                    f"n8n returnerede status {r.status_code}: "
                    f"{r.text[:300]}"
                )
        except requests.RequestException as e:
            st.error(f"Kunne ikke nå webhook: {e}")

# ---------------------------------------------------------------------------
# Polling progress — runs only if a screening was just started for THIS tour.
# Watches n8n's fetch-webhook for new rows matching the run, shows per-competitor
# completion, auto-imports rows to local catalog.db so user can hop straight to
# Review-kandidater.
# ---------------------------------------------------------------------------
inflight = st.session_state.get("screening_in_flight")
if inflight and inflight.get("tour_code") == selected_tour_code:
    from topas_scraper import n8n_client as _n8n  # noqa: PLC0415
    from topas_scraper import catalog_db as _catdb_poll  # noqa: PLC0415

    expected_domains = set(inflight["competitor_domains"])
    expected_count = inflight["expected_count"]
    started_iso = inflight["started_iso"]

    if inflight.get("completed"):
        # Already finished — show summary banner with dismiss button.
        done = inflight.get("done_count", 0)
        missing = sorted(expected_domains - set(inflight.get("done_domains", [])))
        if done >= expected_count:
            st.success(
                f"✓ Screening færdig — alle **{done}/{expected_count}** "
                f"konkurrenter modtaget for {selected_tour_code}. "
                "Gå til **📋 Review-kandidater** for at gennemgå og godkende."
            )
        elif inflight.get("stale_complete"):
            st.success(
                f"✓ n8n er færdig — **{done}/{expected_count}** konkurrenter "
                f"returnerede data for {selected_tour_code}. De resterende "
                f"({', '.join(missing) or '—'}) fandt ingen ture matchende "
                f"søgningen og skrev ikke nogen rækker. "
                "Gå til **📋 Review-kandidater** for at gennemgå."
            )
        else:
            st.warning(
                f"⚠ Screening sluttet med timeout — kun **{done}/{expected_count}** "
                f"konkurrenter kom igennem. Mangler: {', '.join(missing) or '—'}"
            )
        if st.button("Luk status", key=f"close_screen_{selected_tour_code}"):
            del st.session_state["screening_in_flight"]
            st.rerun()
    else:
        # Persist polling progress in session_state så side-navigation ikke
        # nullstiller status. Hvis polling afbrydes (sidehop), fortsætter den
        # fra hvor vi var næste gang siden renders.
        prior_done = set(inflight.get("done_domains", []))
        prior_log = list(inflight.get("log_lines", []))
        prior_elapsed = inflight.get("elapsed", 0)
        prior_seconds_since_new = inflight.get("seconds_since_last_new", 0)

        # Vis straks LAST-KNOWN status (uden at vente på første n8n-fetch)
        # — så brugeren ikke ser tom side når de kommer tilbage til fanen
        initial_done = len(prior_done)
        initial_label = (
            f"⏳ Venter på n8n... {initial_done}/{expected_count} konkurrenter scrapet"
            if initial_done < expected_count
            else f"⏳ Tjekker status... {initial_done}/{expected_count}"
        )
        with st.status(initial_label, expanded=True) as poll_status:
            bar = st.progress(initial_done / expected_count if expected_count else 0)
            stat_line = st.empty()
            log_box = st.empty()

            if prior_done and not (initial_done >= expected_count):
                stat_line.info(
                    f"Genoptager polling fra {initial_done}/{expected_count} "
                    f"(tidligere fremgang gemt i session)"
                )
            if prior_log:
                log_box.code("\n".join(prior_log[-12:]))

            cat_conn_poll = _catdb_poll.connect()
            done_domains: set[str] = set(prior_done)
            log_lines: list[str] = list(prior_log)

            max_wait_seconds = 360       # hård timeout: 6 min
            poll_interval = 8             # poll hver 8s
            stale_after_seconds = 60      # hvis ingen ny række i 60s = n8n færdig
            elapsed = prior_elapsed
            seconds_since_last_new = prior_seconds_since_new
            timeout_hit = False
            stale_complete = False

            while elapsed < max_wait_seconds:
                try:
                    rows = _n8n.fetch_candidates(timeout=15)
                except _n8n.N8nFetchError as exc:
                    stat_line.warning(
                        f"Fetch-webhook fejlede — prøver igen om {poll_interval}s "
                        f"({exc})"
                    )
                    time.sleep(poll_interval)
                    elapsed += poll_interval
                    seconds_since_last_new += poll_interval
                    continue

                # Filter rows belonging to THIS screening run.
                # We compare the first 19 chars (YYYY-MM-DDTHH:MM:SS) so n8n's
                # "...Z"-suffixed timestamps and our naive UTC iso don't collide.
                started_prefix = started_iso[:19]
                new_rows_this_cycle = 0
                for row in rows:
                    if row.get("topasTourCode") != inflight["tour_code"]:
                        continue
                    row_searched = (row.get("searchedAt") or "")[:19]
                    if row_searched < started_prefix:
                        continue
                    domain = row.get("competitorDomain")
                    if domain in expected_domains and domain not in done_domains:
                        done_domains.add(domain)
                        new_rows_this_cycle += 1
                        match_label = (
                            row.get("tourName")
                            or ("(intet match)" if not row.get("hasMatch") else "(uden navn)")
                        )
                        log_lines.append(f"✓ {domain} — {match_label}")
                        # Auto-import so user can review without an extra click
                        try:
                            _catdb_poll.upsert_n8n_candidate(cat_conn_poll, row)
                        except Exception as exc:  # noqa: BLE001
                            log_lines.append(f"   (lokal import-fejl: {exc})")

                if new_rows_this_cycle > 0:
                    seconds_since_last_new = 0
                else:
                    seconds_since_last_new += poll_interval

                done = len(done_domains)
                pct = done / expected_count if expected_count else 0

                # Status-tekst — hvis vi nærmer os "stale-complete", giv brugeren info
                stale_warning = ""
                if done > 0 and seconds_since_last_new >= stale_after_seconds // 2:
                    sec_left = max(0, stale_after_seconds - seconds_since_last_new)
                    stale_warning = f" · ingen nye rækker i {seconds_since_last_new}s (auto-luk om {sec_left}s)"

                stat_line.markdown(
                    f"**{done}/{expected_count}** konkurrenter modtaget · "
                    f"forløbet: **{elapsed}s**  ·  næste check om {poll_interval}s{stale_warning}"
                )
                bar.progress(min(pct, 1.0))
                if log_lines:
                    log_box.code("\n".join(log_lines[-12:]))
                else:
                    log_box.caption("(ingen rækker fra n8n endnu — typisk efter 30-60s)")

                poll_status.update(
                    label=f"⏳ Venter på n8n... {done}/{expected_count} konkurrenter scrapet"
                )

                # PERSIST progress til session_state efter hver cyklus, så
                # side-navigation ikke mister fremgangen.
                inflight["done_domains"] = sorted(done_domains)
                inflight["log_lines"] = log_lines[-50:]   # cap for ikke at vokse uendeligt
                inflight["elapsed"] = elapsed
                inflight["seconds_since_last_new"] = seconds_since_last_new
                st.session_state["screening_in_flight"] = inflight

                # Færdig hvis alle modtaget
                if done >= expected_count:
                    break

                # "Stale complete": vi har set mindst én række, men der er ikke kommet
                # nye i stale_after_seconds — antag n8n er færdig og resten ikke
                # skrev rækker (fx ingen Cypern-tour på Kipling/Vagabond).
                if done > 0 and seconds_since_last_new >= stale_after_seconds:
                    stale_complete = True
                    log_lines.append(
                        f"ℹ Ingen nye rækker i {stale_after_seconds}s — "
                        f"antager n8n er færdig ({done}/{expected_count})"
                    )
                    log_box.code("\n".join(log_lines[-12:]))
                    break

                time.sleep(poll_interval)
                elapsed += poll_interval
            else:
                timeout_hit = True

            # Mark run as completed in session state — next rerun shows summary
            inflight["completed"] = True
            inflight["done_count"] = len(done_domains)
            inflight["done_domains"] = sorted(done_domains)
            inflight["stale_complete"] = stale_complete
            st.session_state["screening_in_flight"] = inflight

            if timeout_hit:
                poll_status.update(
                    label=f"⚠ Timeout — {len(done_domains)}/{expected_count} kom igennem",
                    state="error",
                )
            elif stale_complete:
                poll_status.update(
                    label=f"✓ n8n færdig — {len(done_domains)}/{expected_count} konkurrenter returnerede data",
                    state="complete",
                )
            else:
                poll_status.update(
                    label=f"✓ Færdig — {len(done_domains)}/{expected_count} konkurrenter modtaget",
                    state="complete",
                )

        st.rerun()

# ---------------------------------------------------------------------------
# Comparison views — ONLY shown if this tour has scraped data.
# For unmapped tours (no TARGETS configured), the n8n screening above is the
# next step. Once n8n returns candidates and they're confirmed, the user can
# run a scrape and the comparison views will populate.
# ---------------------------------------------------------------------------
if not selected_tour:
    st.info(
        "👆 Brug screeningen ovenfor for at finde konkurrenter til denne tur. "
        "Når kandidater er bekræftet og scraping er kørt, vises pris-sammenligningen her."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Competitor picker
# ---------------------------------------------------------------------------
st.markdown("### Sammenlign med konkurrent")

# Eligible competitors only — filtered to the selected Topas tour (via competesWith)
# AND with at least one departure
selected_tour_code = (selected_tour or {}).get("code") or selected_code
all_comps_for_tour = [
    c for c in competitors_data
    if c.get("competesWith") == selected_tour_code
       or c.get("competesWith") is None  # backward compat — pre-multi-tour data has no field
]
eligible_comps = [c for c in all_comps_for_tour if c.get("departureCount", 0) > 0]
ineligible_comps = [c for c in all_comps_for_tour if c.get("departureCount", 0) == 0]

if not eligible_comps:
    st.warning("Ingen eligible konkurrenter for denne tour.")
    st.stop()

# Helper utilities ----------------------------------------------------------
def parse_iso(s: str) -> Optional[date]:
    try:
        return date.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def format_dk_date(d: Optional[date]) -> str:
    if d is None:
        return "—"
    months = ["jan", "feb", "mar", "apr", "maj", "jun", "jul", "aug", "sep", "okt", "nov", "dec"]
    return f"{d.day:02d}. {months[d.month - 1]} {d.year}"


# Build picker options. Tre niveauer:
#   1. ALL_OPTION  — alle konkurrenter på tværs af operatører
#   2. OP_PREFIX   — én operatør (alle dens ture aggregeret)
#   3. tour-key    — én specifik konkurrent-tur
# Operator-niveauet vises kun for operatører med >1 tur (ellers er det
# identisk med tour-niveauet).
ALL_OPTION = "__ALL__"
OP_PREFIX = "OP|"


def _comp_key(idx: int, c: dict) -> str:
    return c.get("url") or c.get("tourSlug") or f"{c.get('operator','?')}#{idx}"


comp_by_key: dict[str, dict] = {
    _comp_key(i, c): c for i, c in enumerate(eligible_comps)
}

# Tæl ture per operator → operatører med >1 tur får et samlet picker-option
# OG deres individuelle ture skjules (de ligger samlet i operator-option'et).
# Operatører med kun 1 tur vises som tour-niveau direkte.
from collections import Counter as _Counter
_op_counts = _Counter(c.get("operator", "") for c in eligible_comps)
_multi_ops = sorted(op for op, n in _op_counts.items() if n > 1 and op)
_multi_ops_set = set(_multi_ops)

# Tour-niveau options KUN for operatører med 1 tur (ellers er de allerede
# repræsenteret af operator-option'et).
_single_tour_keys = [
    k for k, c in comp_by_key.items()
    if c.get("operator") not in _multi_ops_set
]

picker_options = (
    [ALL_OPTION]
    + [OP_PREFIX + op for op in _multi_ops]
    + _single_tour_keys
)


def fmt_comp(key: str) -> str:
    if key == ALL_OPTION:
        total = sum(c.get("departureCount", 0) for c in eligible_comps)
        n_operators = len({c.get("operator") for c in eligible_comps if c.get("operator")})
        n_tours = len(eligible_comps)
        if n_tours > n_operators:
            scope = f"{n_operators} op · {n_tours} ture"
        else:
            scope = f"{n_operators} operatører"
        return f"Alle konkurrenter · {scope} · {total} afg."
    if key.startswith(OP_PREFIX):
        op = key[len(OP_PREFIX):]
        op_comps = [c for c in eligible_comps if c.get("operator") == op]
        total = sum(c.get("departureCount", 0) for c in op_comps)
        return f"{op} · alle {len(op_comps)} ture · {total} afg."
    c = comp_by_key[key]
    aller = "● " if c.get("holding") == "Aller Leisure" else ""
    tier = f"T{c.get('tierForPTMD', '?')}"
    n = c.get("departureCount", 0)
    tour_name = c.get("tourName") or c.get("tourSlug") or "(ukendt tur)"
    short_name = tour_name if len(tour_name) <= 40 else tour_name[:37] + "…"
    return f"{aller}{c['operator']} · {short_name} · {tier} · {n} afg."


selected_comp_op = st.radio(
    "Vælg",
    options=picker_options,
    index=0,
    format_func=fmt_comp,
    horizontal=True,
    label_visibility="collapsed",
)

# Tre states downstream:
#   selected_comp is None       → ALL eller operator-mode (multi-comp visning)
#   selected_op_filter is set   → kun den operatørs ture i Markeds-kalender m.v.
#   selected_comp is dict       → enkelt-tur sammenligning (single-pair)
selected_op_filter: Optional[str] = None
if selected_comp_op == ALL_OPTION:
    selected_comp = None
elif selected_comp_op.startswith(OP_PREFIX):
    selected_comp = None
    selected_op_filter = selected_comp_op[len(OP_PREFIX):]
else:
    selected_comp = comp_by_key[selected_comp_op]

# scope_comps: hvilke konkurrent-ture metrics + listings + kalender skal bruge.
# - single tur valgt    → [selected_comp]  (men de fleste single-comp-paths bruger
#                          selected_comp direkte; scope_comps holdes konsistent)
# - operator-filter     → kun den operatørs ture
# - ALL                 → alle eligible
if selected_comp is not None:
    scope_comps = [selected_comp]
elif selected_op_filter:
    scope_comps = [c for c in eligible_comps if c.get("operator") == selected_op_filter]
else:
    scope_comps = eligible_comps

# Show ineligible separately for transparency. Distinguish three cases:
#   - parser_failed: tour has no departures AND no from_price (data extraction failed)
#   - data_limited: tour has from_price but no departures (e.g. Albatros JS-render limit)
#   - product_ineligible: tour has departures but failed eligibility check
if ineligible_comps:
    with st.expander(f"Ekskluderet ({len(ineligible_comps)} ineligible)"):
        for c in ineligible_comps:
            aller = "● " if c.get("holding") == "Aller Leisure" else ""
            tier = c.get("tierForPTMD", "?")
            from_price = c.get("fromPrice")
            n_deps = len(c.get("departures", []))
            notes = c.get("eligibilityNotes", "")

            if n_deps == 0 and from_price:
                reason = f"data-begrænsning · headline {from_price:,} kr.".replace(",", ".")
            elif n_deps == 0:
                reason = "scrape-fejl eller tom side"
            else:
                reason = "produkt ineligible per methodology"

            st.markdown(
                f"- {aller}**{c['operator']}** (T{tier}) — {reason}"
            )
            if notes:
                st.caption(f"  ↳ {notes}")

st.divider()

# ---------------------------------------------------------------------------
# Headline + metrics — adapts to picker (single comp vs Alle)
# ---------------------------------------------------------------------------
topas_deps = selected_tour.get("departures", [])

# For each Topas departure, compute the spread vs each competitor's nearest
# departure within ±14 days. Used by both the single-comp view and the "Alle"
# view (where we average across all eligible competitors).
def compute_pair_spread(topas_dep: dict, comp_deps: list[dict]) -> Optional[int]:
    t_date = parse_iso(topas_dep["startDate"])
    if t_date is None or topas_dep.get("priceDkk") is None:
        return None
    best, best_diff = None, None
    for cd in comp_deps:
        c_date = parse_iso(cd["startDate"])
        if c_date is None or cd.get("priceDkk") is None:
            continue
        diff = abs((c_date - t_date).days)
        if diff <= 14 and (best_diff is None or diff < best_diff):
            best, best_diff = cd, diff
    if best is None:
        return None
    return topas_dep["priceDkk"] - best["priceDkk"]


col_a, col_b, col_c, col_d = st.columns(4)

if selected_comp is not None:
    # Single competitor mode
    comp_deps_view = selected_comp.get("departures", [])
    spreads = [compute_pair_spread(td, comp_deps_view) for td in topas_deps]
    valid_spreads = [s for s in spreads if s is not None]
    sellouts = [d for d in comp_deps_view if d.get("status") == "Udsolgt"]

    with col_a:
        if valid_spreads:
            avg = sum(valid_spreads) // len(valid_spreads)
            st.metric(
                "Snit-spænd",
                f"{avg:+,d} kr.".replace(",", "."),
                help=f"Topas vs {selected_comp['operator']} — {len(valid_spreads)} matchede afgange",
            )
        else:
            st.metric("Snit-spænd", "—", help="Ingen matchede afgange")
    with col_b:
        st.metric("Topas afgange", len(topas_deps))
    with col_c:
        st.metric(f"{selected_comp['operator']} afgange", len(comp_deps_view))
    with col_d:
        st.metric("Konkurrent udsolgt", len(sellouts))

    # Headline
    st.markdown("### Hovedbillede")
    if not valid_spreads:
        st.markdown(
            f"_Ingen overlap mellem Topas og {selected_comp['operator']} inden for ±2 ugers vindue. "
            f"Sammenligning baseres på fra-pris alene._"
        )
    else:
        avg = sum(valid_spreads) // len(valid_spreads)
        abs_avg = abs(avg)
        if abs_avg < 500:
            verdict = f"**Pris-paritet** med {selected_comp['operator']} på de overlappende afgange (snit: {avg:+,d} kr.).".replace(",", ".")
        elif avg < 0:
            verdict = f"Topas ligger **{abs_avg:,d} kr. under** {selected_comp['operator']} pr. departure i snit.".replace(",", ".")
        else:
            verdict = f"Topas ligger **{abs_avg:,d} kr. over** {selected_comp['operator']} pr. departure i snit.".replace(",", ".")
        st.markdown(verdict)
else:
    # "Alle"-mode ELLER operator-mode — aggregate across scope_comps.
    # Markedet defineres af scope_comps: ALL = alle eligible, operator = kun
    # den operatørs ture. Det betyder fx "Albatros · alle 5 ture" giver dig
    # snit-spænd kun mod Albatros' samlede tilbud.
    _scope_label = (
        f"{selected_op_filter}'s {len(scope_comps)} ture"
        if selected_op_filter else f"{len(scope_comps)} konkurrenter"
    )
    all_market_spreads: list[int] = []
    total_market_deps = sum(c.get("departureCount", 0) for c in scope_comps)
    total_market_sellouts = sum(
        sum(1 for d in c.get("departures", []) if d.get("status") == "Udsolgt")
        for c in scope_comps
    )

    for td in topas_deps:
        t_date = parse_iso(td["startDate"])
        if t_date is None or td.get("priceDkk") is None:
            continue
        nearby_prices = []
        for c in scope_comps:
            for cd in c.get("departures", []):
                c_date = parse_iso(cd["startDate"])
                if c_date is None or cd.get("priceDkk") is None:
                    continue
                if abs((c_date - t_date).days) <= 14:
                    nearby_prices.append(cd["priceDkk"])
        if nearby_prices:
            market_avg = sum(nearby_prices) // len(nearby_prices)
            all_market_spreads.append(td["priceDkk"] - market_avg)

    with col_a:
        if all_market_spreads:
            avg = sum(all_market_spreads) // len(all_market_spreads)
            label = "Snit-spænd vs " + (selected_op_filter if selected_op_filter else "marked")
            st.metric(
                label,
                f"{avg:+,d} kr.".replace(",", "."),
                help=f"Topas-pris minus snit af {_scope_label}, beregnet over {len(all_market_spreads)} af Topas's afgange (±14 dage).",
            )
        else:
            st.metric("Snit-spænd", "—")
    with col_b:
        st.metric("Topas afgange", len(topas_deps))
    with col_c:
        col_c_label = f"{selected_op_filter} afgange" if selected_op_filter else "Marked afgange"
        st.metric(col_c_label, total_market_deps, help=f"Sum af afgange fra {_scope_label}")
    with col_d:
        col_d_label = f"{selected_op_filter} udsolgt" if selected_op_filter else "Marked udsolgt"
        st.metric(col_d_label, total_market_sellouts)

    st.markdown("### Hovedbillede")
    if not all_market_spreads:
        st.markdown("_Ingen overlap med markedet inden for ±2 ugers vindue._")
    else:
        avg = sum(all_market_spreads) // len(all_market_spreads)
        abs_avg = abs(avg)
        if abs_avg < 500:
            verdict = f"**Pris-paritet** med markedet i snit (Topas {avg:+,d} kr. vs markeds-snit).".replace(",", ".")
        elif avg < 0:
            verdict = f"Topas ligger **{abs_avg:,d} kr. under markedet** i snit på de overlappende afgange.".replace(",", ".")
        else:
            verdict = f"Topas ligger **{abs_avg:,d} kr. over markedet** i snit på de overlappende afgange.".replace(",", ".")
        st.markdown(verdict)

# ---------------------------------------------------------------------------
# Full departure listings (Topas + competitor(s))
# ---------------------------------------------------------------------------
st.divider()
st.markdown("### Alle afgange")

col_topas, col_comp = st.columns(2)

with col_topas:
    st.markdown(f"**Topas — {len(topas_deps)} afgange**")
    topas_rows = [{
        "Dato": format_dk_date(parse_iso(d["startDate"])),
        "Pris": d.get("priceDkk"),
        "Status": d.get("status", ""),
    } for d in sorted(topas_deps, key=lambda x: x["startDate"])]
    st.dataframe(
        pd.DataFrame(topas_rows),
        use_container_width=True,
        hide_index=True,
        column_config={"Pris": st.column_config.NumberColumn(format="%d kr.")},
    )

with col_comp:
    if selected_comp is not None:
        # Single-comp view
        comp_deps_view = selected_comp.get("departures", [])
        comp_url = selected_comp.get("url")
        header_text = f"**{selected_comp['operator']} — {len(comp_deps_view)} afgange**"
        if comp_url:
            st.markdown(f"{header_text} — [åbn side ↗]({comp_url})")
        else:
            st.markdown(header_text)
        _comp_cat = _CATEGORY_LOOKUP.get(comp_url, "—")
        comp_rows = [{
            "Dato": format_dk_date(parse_iso(d["startDate"])),
            "Pris": d.get("priceDkk"),
            "Status": d.get("status", ""),
            "Kategori": _comp_cat,
            "Afgang fra": d.get("flightOrigin") or "",
        } for d in sorted(comp_deps_view, key=lambda x: x["startDate"])]
        st.dataframe(
            pd.DataFrame(comp_rows),
            use_container_width=True,
            hide_index=True,
            column_config={"Pris": st.column_config.NumberColumn(format="%d kr.")},
        )
    else:
        # All-comps view (eller operator-filtered) — vis alle scope_comps' afgange
        # sorteret kronologisk, med operator labelled pr. række.
        market_deps_total = sum(c.get("departureCount", 0) for c in scope_comps)
        if selected_op_filter:
            st.markdown(f"**{selected_op_filter} — {len(scope_comps)} ture · {market_deps_total} afgange**")
        else:
            st.markdown(f"**Alle konkurrenter — {market_deps_total} afgange**")
        all_market_rows = []
        for c in scope_comps:
            for d in c.get("departures", []):
                all_market_rows.append({
                    "Operatør": c["operator"],
                    "Tur": c.get("tourName") or c.get("name") or "",
                    "Kategori": _CATEGORY_LOOKUP.get(c.get("url"), "—"),
                    "Dato": format_dk_date(parse_iso(d["startDate"])),
                    "_sort": d["startDate"],
                    "Pris": d.get("priceDkk"),
                    "Status": d.get("status", ""),
                    "Afgang fra": d.get("flightOrigin") or "",
                })
        all_market_rows.sort(key=lambda r: r["_sort"])
        # Drop sort key for display
        for r in all_market_rows:
            r.pop("_sort", None)
        st.dataframe(
            pd.DataFrame(all_market_rows),
            use_container_width=True,
            hide_index=True,
            column_config={"Pris": st.column_config.NumberColumn(format="%d kr.")},
        )


# ---------------------------------------------------------------------------
# Calendar overview — flat chronological list of ALL departures across operators
# ---------------------------------------------------------------------------
st.divider()
if selected_op_filter is not None:
    _n_tours = len(calendar_comps)
    st.markdown(f"### Markeds-kalender · Topas vs {selected_op_filter} ({_n_tours} ture)")
    st.caption(
        f"Topas-afgange og alle {_n_tours} {selected_op_filter}-ture, sorteret efter dato. "
        "**Topas-rækker** er fed-skrift. Status-farver er fra prissætter-perspektiv: "
        "🟢 Udsolgt = stærk efterspørgsel · 🟡 Garanteret = sælger som planlagt · "
        "🔴 Åben = sælger ikke endnu (potentielt prisproblem)."
    )
elif selected_comp is None:
    st.markdown("### Markeds-kalender · alle operatører kronologisk")
    st.caption(
        "Alle afgange fra Topas + konkurrenter, sorteret efter dato. "
        "**Topas-rækker** er fed-skrift. Status-farver er fra prissætter-perspektiv: "
        "🟢 Udsolgt = stærk efterspørgsel · 🟡 Garanteret = sælger som planlagt · "
        "🔴 Åben = sælger ikke endnu (potentielt prisproblem)."
    )
else:
    st.markdown(f"### Markeds-kalender · Topas vs {selected_comp['operator']}")
    st.caption(
        f"Topas-afgange og {selected_comp['operator']}-afgange, sorteret efter dato. "
        "**Topas-rækker** er fed-skrift. Status-farver er fra prissætter-perspektiv: "
        "🟢 Udsolgt = stærk efterspørgsel · 🟡 Garanteret = sælger som planlagt · "
        "🔴 Åben = sælger ikke endnu (potentielt prisproblem)."
    )


def _format_delta(delta_kr: int | float | None, observed_at: str | None) -> str:
    """Format pris-ændring som '↑ 500 (30/4)' / '↓ 300 (28/4)' / '—'."""
    if delta_kr is None or delta_kr == 0:
        return "—"
    arrow = "↑" if delta_kr > 0 else "↓"
    amount = abs(int(delta_kr))
    # Format previous date kort som 'd/m'
    short_date = ""
    if observed_at:
        try:
            obs_dt = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
            short_date = f" ({obs_dt.day}/{obs_dt.month})"
        except (ValueError, TypeError):
            pass
    return f"{arrow} {amount:,} kr.{short_date}".replace(",", ".")


def _format_meals(tour_record) -> str:
    """Render meal info from a tour/comp record into a short Danish label."""
    if not tour_record:
        return "—"
    desc = (tour_record.get("mealsDescription") or "").strip()
    count = tour_record.get("mealsIncluded")
    if desc:
        if isinstance(count, int) and count > 0 and count not in (99, -1):
            return f"{desc} ({count})"
        return desc
    if isinstance(count, int):
        if count == 99:
            return "All Inclusive"
        if count > 0:
            return f"{count} måltider"
    return "—"


def _month_key(iso: str) -> Optional[str]:
    d = parse_iso(iso)
    if d is None:
        return None
    return f"{d.year}-{d.month:02d}"


def _month_label(key: str) -> str:
    months = ["jan", "feb", "mar", "apr", "maj", "jun", "jul", "aug", "sep", "okt", "nov", "dec"]
    year, month = key.split("-")
    return f"{months[int(month) - 1]} {year}"


# Status -> background color (pricing-perspective)
def _status_bg_color(status: str) -> str:
    """Background color for status cell. Pricing-perspective:
       Udsolgt = green (good demand signal),
       Garanteret/Få pladser = yellow (selling as planned),
       Åben/On-request = red (potentially priced wrong)."""
    s = (status or "").strip().lower()
    if s == "udsolgt":
        return "#d4edda"   # soft green
    if s in ("garanteret", "få pladser"):
        return "#fff3cd"   # soft yellow
    if s in ("åben", "ledig", "afventer pris", "på forespørgsel"):
        return "#f8d7da"   # soft red
    return "#ffffff"


# Build flat list of all departures across all operators
all_departures: list[dict] = []

for d in topas_deps:
    parsed = parse_iso(d["startDate"])
    if parsed is None:
        continue
    _topas_name = (selected_tour.get("name") if selected_tour else "") or selected_catalog_tour.get("tour_name") or ""
    _topas_url = (selected_tour.get("url") if selected_tour else "") or selected_catalog_tour.get("url") or ""
    all_departures.append({
        "_sort_date": parsed,
        "_month_key": _month_key(d["startDate"]),
        "Operatør": "Topas",
        "Tur": _topas_name,
        "_url": _topas_url,
        "Kategori": _auto_category_label(_topas_name, _topas_url),
        "Dage": (
            selected_tour.get("durationDays") if selected_tour
            else selected_catalog_tour.get("duration_days")
        ),
        "_is_topas": True,
        "_delta": d.get("priceDelta"),
        "_delta_observed_at": d.get("priceDeltaObservedAt"),
        "Dato": format_dk_date(parsed),
        "Pris": d.get("priceDkk"),
        "Status": d.get("status", ""),
        "Måltider": _format_meals(selected_tour),
        "Detaljer": "",
    })

# Markeds-kalender bruger scope_comps direkte — det er allerede sat efter pickeren
# til at respektere ALL/operator/single-tur-modes konsistent.
calendar_comps = scope_comps

for comp in calendar_comps:
    for d in comp.get("departures", []):
        parsed = parse_iso(d["startDate"])
        if parsed is None:
            continue
        # For competitors, "Detaljer" shows flight origin
        details_bits = []
        if d.get("flightOrigin"):
            details_bits.append(f"fra {d['flightOrigin']}")
        all_departures.append({
            "_sort_date": parsed,
            "_month_key": _month_key(d["startDate"]),
            "Operatør": _canonical_op(comp["operator"]),
            "Tur": comp.get("tourName") or comp.get("name") or "",
            "_url": comp.get("url") or "",
            "Kategori": _CATEGORY_LOOKUP.get(comp.get("url"), "—"),
            "Dage": comp.get("durationDays"),
            "_is_topas": False,
            "Dato": format_dk_date(parsed),
            "Pris": d.get("priceDkk"),
            "_delta": d.get("priceDelta"),
            "_delta_observed_at": d.get("priceDeltaObservedAt"),
            "Status": d.get("status", ""),
            "Måltider": _format_meals(comp),
            "Detaljer": " · ".join(details_bits),
        })

# Sort chronologically
all_departures.sort(key=lambda x: x["_sort_date"])

# Group by month and render with sub-headers
if not all_departures:
    st.info("Ingen afgange at vise.")
else:
    # Group into per-month buckets
    months_seen: list[str] = []
    by_month: dict[str, list[dict]] = {}
    for dep in all_departures:
        mk = dep["_month_key"]
        if mk not in by_month:
            by_month[mk] = []
            months_seen.append(mk)
        by_month[mk].append(dep)

    # Render each month as its own styled dataframe rendered via HTML, så
    # vi kan have klikbare links direkte i Tur-cellen.
    # Streamlit's st.dataframe + LinkColumn understøtter ikke per-række display-tekst,
    # så vi går vejen via pandas Styler.to_html() + st.markdown(unsafe_allow_html=True).
    import html as _html  # noqa: PLC0415
    display_cols = ["Operatør", "Tur", "Kategori", "Dage", "Dato", "Pris", "Status", "Måltider", "Detaljer"]

    def _tur_link_html(name: str, url: str) -> str:
        """Lav Tur-cellen til en klikbar <a>. Hvis ingen URL, vis bare navnet."""
        safe_name = _html.escape(name or "")
        if not url:
            return safe_name
        safe_url = _html.escape(url, quote=True)
        return f'<a href="{safe_url}" target="_blank" rel="noopener">{safe_name}</a>'

    def _fmt_price_dk(v) -> str:
        if v == "" or v is None:
            return "—"
        try:
            return f"{int(v):,} kr.".replace(",", ".")
        except (TypeError, ValueError):
            return "—"

    for mk in months_seen:
        rows = by_month[mk]
        st.markdown(f"#### {_month_label(mk)} · {len(rows)} afgange")

        is_topas_flags = [bool(r.get("_is_topas")) for r in rows]
        df_rows = []
        for r in rows:
            row_dict = {c: r.get(c, "") for c in display_cols}
            # Erstat Tur-cellen med <a href> HTML
            row_dict["Tur"] = _tur_link_html(r.get("Tur", ""), r.get("_url", ""))
            df_rows.append(row_dict)
        df = pd.DataFrame(df_rows, columns=display_cols)

        def _style_row(row, _flags=is_topas_flags):
            base = "font-weight: bold;" if _flags[row.name] else ""
            return [base] * len(row)

        def _style_status(col):
            if col.name != "Status":
                return [""] * len(col)
            return [f"background-color: {_status_bg_color(v)}" for v in col]

        # Eksplicitte column-widths sikrer at månedstabellerne er aligned
        # på tværs (alle juni-rækker står på linje med alle juli-rækker etc.).
        # Dage/Pris er højre-justeret som standard for tal-kolonner.
        col_widths = {
            "Operatør": "11%",
            "Tur": "23%",
            "Kategori": "9%",
            "Dage": "5%",
            "Dato": "10%",
            "Pris": "9%",
            "Status": "9%",
            "Måltider": "16%",
            "Detaljer": "8%",
        }
        col_aligns = {
            "Dage": "right",
            "Pris": "right",
            "Dato": "left",
            "Status": "center",
        }

        col_styles = []
        for i, c in enumerate(display_cols, start=1):
            w = col_widths.get(c)
            a = col_aligns.get(c, "left")
            props = f"text-align: {a};"
            if w:
                props = f"width: {w}; {props}"
            col_styles.append({
                "selector": f"th:nth-child({i}), td:nth-child({i})",
                "props": props,
            })

        styled = (
            df.style
            .apply(_style_row, axis=1)
            .apply(_style_status, axis=0)
            .format({"Pris": _fmt_price_dk})
            .hide(axis="index")
            .set_table_styles([
                {"selector": "table", "props": "border-collapse: collapse; width: 100%; max-width: 1500px; font-size: 14px; table-layout: fixed;"},
                {"selector": "th", "props": "padding: 6px 10px; background: #fafafa; border-bottom: 1px solid #e0e0e0; font-weight: 600;"},
                {"selector": "td", "props": "padding: 6px 10px; border-bottom: 1px solid #f0f0f0; vertical-align: top;"},
                {"selector": "a", "props": "color: #2563eb; text-decoration: none;"},
                {"selector": "a:hover", "props": "text-decoration: underline;"},
                *col_styles,
            ])
        )

        st.markdown(styled.to_html(escape=False), unsafe_allow_html=True)
