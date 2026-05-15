"""
Ugentlig rapport — diff-detection over valgbart tidsvindue.

Bygges udelukkende fra snapshots-tabellen i snapshots.db.
Viser hvad der er ændret siden brugeren sidst kiggede:
  - Pris-ændringer (op/ned)
  - Status-ændringer
  - Nye afgange
  - Forsvundne afgange (incl. anomaly-klassifikation)
  - Bemærkelsesværdige overgange (withdrawn / fast_sellout)

Bruges typisk hver mandag morgen efter weekend-scrape.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Ugentlig rapport · Topas", page_icon="📊", layout="wide")

# Password-gate — stopper page-rendering indtil korrekt adgangskode.
from topas_scraper._auth import require_auth  # noqa: E402
require_auth()


st.markdown("# 📊 Ugentlig rapport")
st.caption(
    "Hvad er ændret siden sidst? Sammenligner aktuelle snapshots med tidligere "
    "observationer og fremhæver de prishændelser, status-ændringer og afgangs-bevægelser "
    "der er værd at reagere på."
)

# ---------------------------------------------------------------------------
# Data load — generér dashboard-payload fra Supabase ved hver page-load.
#
# Samme pattern som Tour-detalje: kør export() til /tmp og læs derfra. På den
# måde afspejler rapporten altid den nyeste DB-state, uanset om brugeren har
# været forbi Tour-detalje først. Cached i 10 min så rapporten loader hurtigt
# ved gentagne page-views.
# ---------------------------------------------------------------------------

JSON_PATH = Path(tempfile.gettempdir()) / "topas_dashboard.json"


@st.cache_data(ttl=600)
def _load_dashboard() -> Optional[dict]:
    """Generér dashboard-payload fra Supabase. Cached 10 min for hurtig nav."""
    try:
        from topas_scraper.export import export as _export  # noqa: PLC0415
        _export(output=JSON_PATH)
        return json.loads(JSON_PATH.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        st.error(
            f"⚠ Export fra Supabase fejlede ({type(exc).__name__}). "
            "Falder tilbage til committed `data/dashboard.json` (kan være gammel). "
            "Tjek server-logs for detaljer."
        )
        # Vis kun fuld trace lokalt — kan lække DSN-fragmenter i prod.
        if os.getenv("APP_DEBUG"):
            import traceback  # noqa: PLC0415
            with st.expander("Stack trace (debug)", expanded=False):
                st.code(traceback.format_exc())

        fallback = Path("data/dashboard.json")
        if fallback.exists():
            try:
                return json.loads(fallback.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return None
        return None


data = _load_dashboard()
if data is None:
    st.warning("Ingen scraped data. Kør en scrape først via Tour-detalje-siden eller CLI.")
    st.stop()

tours = data.get("tours", [])
competitors = data.get("competitors", [])

# ---------------------------------------------------------------------------
# Window selector
# ---------------------------------------------------------------------------

col_window, col_meta = st.columns([2, 3])

with col_window:
    window_label = st.selectbox(
        "Tidsvindue",
        options=["Sidste 7 dage", "Sidste 14 dage", "Sidste 30 dage", "Sidste 90 dage"],
        index=0,
        help="Hvor langt tilbage skal vi sammenligne mod for at finde ændringer?",
    )

window_days = {"Sidste 7 dage": 7, "Sidste 14 dage": 14,
               "Sidste 30 dage": 30, "Sidste 90 dage": 90}[window_label]
cutoff_dt = datetime.utcnow() - timedelta(days=window_days)

with col_meta:
    snap_at = data.get("snapshotTakenAt", "")
    try:
        snap_dt = datetime.fromisoformat(snap_at.replace("Z", "+00:00"))
        snap_pretty = snap_dt.strftime("%d. %b %Y · kl. %H:%M")
    except (ValueError, TypeError):
        snap_pretty = snap_at
    st.caption(f"Seneste scrape: **{snap_pretty}** · Vindue: **{window_days} dage** "
               f"(siden {cutoff_dt.strftime('%d. %b %Y')})")

st.divider()


def _parse_iso(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00").replace("+00:00", ""))
    except (ValueError, TypeError):
        return None


def _format_dk_date(d: datetime | date | None) -> str:
    if d is None:
        return "—"
    months = ["jan", "feb", "mar", "apr", "maj", "jun",
              "jul", "aug", "sep", "okt", "nov", "dec"]
    return f"{d.day:02d}. {months[d.month - 1]} {d.year}"


# ---------------------------------------------------------------------------
# Aggregate findings across all competitors + tours
# ---------------------------------------------------------------------------

price_changes: list[dict] = []   # priser der er ændret
status_changes: list[dict] = []  # status-overgange (incl. anomalies)
new_departures: list[dict] = []  # afgange der dukkede op i vinduet
anomalies: list[dict] = []        # withdrawn / fast_sellout markeret af export

# Build a tour-code → name lookup for context
tour_name_by_code = {t.get("code"): t.get("name") for t in tours if t.get("code")}


def _tour_existed_before_window(deps: list[dict]) -> bool:
    """Returnerer True hvis vi har data for denne tour fra FØR window-cutoff.

    Bruges til "Nye afgange"-detection: en afgang er kun "ny" hvis touren
    selv har eksisteret i vores katalog længere end vinduet. Ellers fanger
    vi falske-positive fra helt nyligt scrapede tours hvor ALLE afgange
    har firstSeen inden for vinduet bare fordi tour-data først blev hentet
    da.
    """
    for d in deps:
        fs = _parse_iso(d.get("firstSeen"))
        if fs is not None and fs < cutoff_dt:
            return True
    return False


def _process_departures(operator: str, tour_code: str, tour_name: str, deps: list[dict]) -> None:
    tour_existed = _tour_existed_before_window(deps)
    for d in deps:
        start_iso = d.get("startDate")
        start_dt = _parse_iso(start_iso) if start_iso else None
        # Pris-ændring (har vi priceDelta-data?)
        # Topas's egne pris-ændringer udelades — rapporten handler om markedet
        # (konkurrenter), og Gorm kender allerede sine egne pris-skift.
        if operator != "Topas" and d.get("priceDelta") is not None:
            obs_at = _parse_iso(d.get("priceDeltaObservedAt"))
            if obs_at and obs_at >= cutoff_dt:
                price_changes.append({
                    "operator": operator,
                    "tour_code": tour_code,
                    "tour_name": tour_name,
                    "start_date": start_iso,
                    "delta": d["priceDelta"],
                    "previous_price": d.get("priceDeltaPrevious"),
                    "current_price": d.get("priceDkk"),
                    "previous_observed_at": d.get("priceDeltaObservedAt"),
                    "days_ago": d.get("priceDeltaDaysAgo"),
                })

        # Status-anomali (withdrawn / fast_sellout)
        anomaly = d.get("statusAnomaly")
        if anomaly:
            obs_at = _parse_iso(anomaly.get("current_observed_at"))
            if obs_at is None or obs_at >= cutoff_dt:
                anomalies.append({
                    "operator": operator,
                    "tour_code": tour_code,
                    "tour_name": tour_name,
                    "start_date": start_iso,
                    "type": anomaly.get("anomaly_type"),
                    "label": anomaly.get("label"),
                    "previous_state": anomaly.get("previous_state"),
                    "current_state": anomaly.get("current_state"),
                    "previous_price": anomaly.get("previous_price_dkk"),
                    "current_price": anomaly.get("current_price_dkk"),
                    "previous_observed_at": anomaly.get("previous_observed_at"),
                    "changed_at": anomaly.get("current_observed_at"),
                    "severity": anomaly.get("severity"),
                })

        # Nye afgange — afgang dukket op INDEN FOR vinduet for en tour der
        # allerede eksisterede FØR vinduet. Hvis touren selv er ny i vinduet,
        # skipper vi (ellers tæller vi 117 "nye afgange" hvergang vi første
        # gang scraper en konkurrent — ikke det Gorm vil have).
        first_seen_str = d.get("firstSeen")
        if first_seen_str and not d.get("isArchived") and tour_existed:
            first_seen_dt = _parse_iso(first_seen_str)
            today = datetime.utcnow()
            is_future = start_dt is not None and start_dt > today
            if first_seen_dt and first_seen_dt >= cutoff_dt and is_future:
                new_departures.append({
                    "operator": operator,
                    "tour_code": tour_code,
                    "tour_name": tour_name,
                    "start_date": start_iso,
                    "first_seen": first_seen_str,
                    "current_status": d.get("status"),
                    "current_price": d.get("priceDkk"),
                })


# Iterate Topas tours
for t in tours:
    code = t.get("code")
    name = t.get("name", "")
    deps = t.get("departures", [])
    _process_departures("Topas", code, name, deps)

# Iterate competitors
for c in competitors:
    op = c.get("operator", "")
    code = c.get("competesWith") or "?"
    name = c.get("tourName") or tour_name_by_code.get(code) or ""
    deps = c.get("departures", [])
    _process_departures(op, code, name, deps)


# ---------------------------------------------------------------------------
# Top-line metrics
# ---------------------------------------------------------------------------

m1, m2, m3, m4 = st.columns(4)
m1.metric("🚨 Bemærkelsesværdige", len(anomalies),
          help="Withdrawn fra salg eller hurtigt udsolgt — kræver opmærksomhed")
m2.metric("Δ Pris-ændringer", len(price_changes))
m3.metric("🆕 Nye afgange", len(new_departures),
          help="Nye afgange i tours vi allerede havde data om før vinduet — fx en ekstra afgang åbnet for salg")
m4.metric("Total fund", len(anomalies) + len(price_changes) + len(new_departures))

st.divider()


# ---------------------------------------------------------------------------
# 🚨 Bemærkelsesværdige ændringer (anomalies)
# ---------------------------------------------------------------------------

if anomalies:
    st.markdown("## 🚨 Bemærkelsesværdige ændringer")
    for a in sorted(anomalies, key=lambda x: x.get("severity", "low") + x.get("changed_at", ""), reverse=True):
        sev_emoji = "🚨" if a["severity"] == "high" else "⚡"
        # Beregn tidsvindue mellem sidste observation af forrige status og nuværende
        prev_obs_dt = _parse_iso(a.get("previous_observed_at"))
        curr_obs_dt = _parse_iso(a.get("changed_at"))
        if prev_obs_dt and curr_obs_dt:
            delta = curr_obs_dt - prev_obs_dt
            window_seconds = int(delta.total_seconds())
            window_days = delta.days
            same_day = prev_obs_dt.date() == curr_obs_dt.date()
        else:
            window_seconds = None
            window_days = None
            same_day = False

        # Når begge observationer er samme dag, vis klokkeslæt i stedet for
        # bare datoen — ellers ser det forvirrende ud at se "13. maj 2026"
        # i begge kolonner.
        def _fmt_obs(dt):
            if dt is None:
                return "—"
            if same_day:
                return f"{dt.strftime('%H:%M')}, {_format_dk_date(dt)}"
            return _format_dk_date(dt)

        with st.container(border=True):
            cols = st.columns([3, 2, 2, 2])
            with cols[0]:
                st.markdown(f"**{sev_emoji} {a['operator']}** · {a['tour_code']} · {a['tour_name']}")
                start_dt = _parse_iso(a["start_date"])
                st.caption(f"Afgang: **{_format_dk_date(start_dt)}**")
            with cols[1]:
                st.caption(f"Var (sidst set {_fmt_obs(prev_obs_dt)})")
                st.write(f"{a['previous_state']}")
                if a.get("previous_price"):
                    st.caption(f"{a['previous_price']:,} kr.".replace(",", "."))
            with cols[2]:
                st.caption(f"Blev til (set {_fmt_obs(curr_obs_dt)})")
                st.write(f"**{a.get('current_state') or '(forsvundet)'}**")
                if a.get("current_price"):
                    st.caption(f"{a['current_price']:,} kr.".replace(",", "."))
            with cols[3]:
                st.caption("Skift inden for")
                if window_seconds is None:
                    st.write("—")
                elif window_seconds < 3600:
                    st.write(f"**{window_seconds // 60} min**")
                elif window_seconds < 86400:
                    st.write(f"**{window_seconds // 3600} timer**")
                elif window_days == 1:
                    st.write("**1 dag**")
                else:
                    st.write(f"**{window_days} dage**")

    st.divider()


# ---------------------------------------------------------------------------
# Δ Pris-ændringer (op/ned)
# ---------------------------------------------------------------------------

if price_changes:
    price_drops = sorted([p for p in price_changes if p["delta"] < 0], key=lambda x: x["delta"])
    price_rises = sorted([p for p in price_changes if p["delta"] > 0], key=lambda x: -x["delta"])

    if price_drops:
        st.markdown(f"## 🔻 Pris-fald ({len(price_drops)})")
        st.caption("Konkurrenter der har sænket priser i vinduet — overvej om Topas skal følge.")
        df_drops = pd.DataFrame([
            {
                "Operatør": p["operator"],
                "Tur-kode": p["tour_code"],
                "Tur": p["tour_name"][:60],
                "Afgang": _format_dk_date(_parse_iso(p["start_date"])),
                "Var": f"{p['previous_price']:,} kr.".replace(",", ".") if p.get("previous_price") else "—",
                "Nu": f"{p['current_price']:,} kr.".replace(",", ".") if p.get("current_price") else "—",
                "∆": f"{p['delta']:+,} kr.".replace(",", "."),
                "Dage siden": p.get("days_ago"),
            }
            for p in price_drops
        ])
        st.dataframe(df_drops, use_container_width=True, hide_index=True)

    if price_rises:
        st.markdown(f"## 🔺 Pris-stigninger ({len(price_rises)})")
        st.caption("Konkurrenter der har hævet priser — kan signalere stærk efterspørgsel.")
        df_rises = pd.DataFrame([
            {
                "Operatør": p["operator"],
                "Tur-kode": p["tour_code"],
                "Tur": p["tour_name"][:60],
                "Afgang": _format_dk_date(_parse_iso(p["start_date"])),
                "Var": f"{p['previous_price']:,} kr.".replace(",", ".") if p.get("previous_price") else "—",
                "Nu": f"{p['current_price']:,} kr.".replace(",", ".") if p.get("current_price") else "—",
                "∆": f"{p['delta']:+,} kr.".replace(",", "."),
                "Dage siden": p.get("days_ago"),
            }
            for p in price_rises
        ])
        st.dataframe(df_rises, use_container_width=True, hide_index=True)

    st.divider()


# ---------------------------------------------------------------------------
# 🆕 Nye afgange (første gang set inden for tidsvinduet)
# ---------------------------------------------------------------------------

if new_departures:
    st.markdown(f"## 🆕 Nye afgange ({len(new_departures)})")
    st.caption(
        "Afgange der er dukket op i vinduet for tours vi allerede tracker — "
        "fx operatøren har åbnet en ekstra dato eller en ny sæson. "
        "Tours scraped første gang i vinduet er ekskluderet (alle deres "
        "afgange ville være 'nye' ellers)."
    )
    df_new = pd.DataFrame([
        {
            "Operatør": n["operator"],
            "Tur-kode": n["tour_code"],
            "Tur": n["tour_name"][:60],
            "Afgang": _format_dk_date(_parse_iso(n["start_date"])),
            "Først set": _format_dk_date(_parse_iso(n["first_seen"])),
            "Status": n.get("current_status") or "—",
            "Pris": f"{n['current_price']:,} kr.".replace(",", ".") if n.get("current_price") else "—",
        }
        for n in sorted(new_departures, key=lambda x: x.get("first_seen", ""), reverse=True)
    ])
    st.dataframe(df_new, use_container_width=True, hide_index=True)

    st.divider()


# ---------------------------------------------------------------------------
# Empty state
# ---------------------------------------------------------------------------

if not (anomalies or price_changes or new_departures):
    st.info(
        "Ingen ændringer i vinduet. Det kan skyldes at:\n"
        "  - Du har ikke kørt nye scrapes i perioden\n"
        "  - Markedet er reelt roligt\n"
        "  - Tidsvinduet er for kort\n\n"
        "Prøv at vælge et længere vindue, eller kør en scrape fra Tour-detalje-siden."
    )

    st.divider()
