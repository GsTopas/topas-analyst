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
            f"⚠ Export fra Supabase fejlede: `{type(exc).__name__}: {exc}`\n\n"
            "Falder tilbage til committed `data/dashboard.json` (kan være gammel)."
        )
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
vanished: list[dict] = []         # afgange der forsvandt i vinduet
anomalies: list[dict] = []        # withdrawn / fast_sellout markeret af export

# Build a tour-code → name lookup for context
tour_name_by_code = {t.get("code"): t.get("name") for t in tours if t.get("code")}


def _process_departures(operator: str, tour_code: str, tour_name: str, deps: list[dict]) -> None:
    for d in deps:
        start_iso = d.get("startDate")
        start_dt = _parse_iso(start_iso) if start_iso else None
        # Drop very old departures (already past) for "vanished" and "new" sections
        # but keep them for price/status delta tracking.

        # Pris-ændring (har vi priceDelta-data?)
        if d.get("priceDelta") is not None:
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

        # Forsvundne afgange (isArchived=true OG datoen er stadig fremtidig =
        # interessant signal)
        if d.get("isArchived"):
            today = datetime.utcnow()
            is_future = start_dt is not None and start_dt > today
            if is_future:
                vanished.append({
                    "operator": operator,
                    "tour_code": tour_code,
                    "tour_name": tour_name,
                    "start_date": start_iso,
                    "last_status": d.get("status"),
                    "last_price": d.get("priceDkk"),
                    "last_seen_run": d.get("lastSeenRun"),
                })

        # Nye afgange — første observation er inden for vinduet OG datoen er
        # fremtidig. firstSeen sættes af export() ud fra ældste snapshot.
        first_seen_str = d.get("firstSeen")
        if first_seen_str and not d.get("isArchived"):
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

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("🚨 Bemærkelsesværdige", len(anomalies),
          help="Withdrawn fra salg eller hurtigt udsolgt — kræver opmærksomhed")
m2.metric("Δ Pris-ændringer", len(price_changes))
m3.metric("🆕 Nye afgange", len(new_departures),
          help="Afgange første gang set inden for tidsvinduet")
m4.metric("🗓️ Fjernede afgange", len(vanished),
          help="Afgange der ikke længere er på operatørens side, men hvor datoen stadig er fremtidig")
m5.metric("Total fund", len(anomalies) + len(price_changes) + len(new_departures) + len(vanished))

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
            window_days = (curr_obs_dt - prev_obs_dt).days
        else:
            window_days = None

        with st.container(border=True):
            cols = st.columns([3, 2, 2, 2])
            with cols[0]:
                st.markdown(f"**{sev_emoji} {a['operator']}** · {a['tour_code']} · {a['tour_name']}")
                start_dt = _parse_iso(a["start_date"])
                st.caption(f"Afgang: **{_format_dk_date(start_dt)}**")
            with cols[1]:
                st.caption(f"Var (sidst set {_format_dk_date(prev_obs_dt)})")
                st.write(f"{a['previous_state']}")
                if a.get("previous_price"):
                    st.caption(f"{a['previous_price']:,} kr.".replace(",", "."))
            with cols[2]:
                st.caption(f"Blev til (set {_format_dk_date(curr_obs_dt)})")
                st.write(f"**{a.get('current_state') or '(forsvundet)'}**")
                if a.get("current_price"):
                    st.caption(f"{a['current_price']:,} kr.".replace(",", "."))
            with cols[3]:
                st.caption("Skift inden for")
                if window_days is not None:
                    if window_days == 0:
                        st.write("**< 1 dag**")
                    elif window_days == 1:
                        st.write("**1 dag**")
                    else:
                        st.write(f"**{window_days} dage**")
                else:
                    st.write("—")

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
        "Afgange der ikke var i kataloget før vinduet startede — kan signalere at "
        "operatøren har åbnet nye datoer eller en sæson."
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
# 🗓️ Fjernede afgange (kun fremtidige)
# ---------------------------------------------------------------------------

if vanished:
    st.markdown(f"## 🗓️ Fjernede afgange ({len(vanished)})")
    st.caption(
        "Afgange der ikke længere er på operatørens side, men hvor datoen stadig "
        "er i fremtiden. Sandsynlig årsag: udsolgt eller annulleret."
    )
    df_vanished = pd.DataFrame([
        {
            "Operatør": v["operator"],
            "Tur-kode": v["tour_code"],
            "Tur": v["tour_name"][:60],
            "Afgang": _format_dk_date(_parse_iso(v["start_date"])),
            "Sidst set som": v.get("last_status") or "—",
            "Sidste pris": f"{v['last_price']:,} kr.".replace(",", ".") if v.get("last_price") else "—",
        }
        for v in sorted(vanished, key=lambda x: x.get("start_date", ""))
    ])
    st.dataframe(df_vanished, use_container_width=True, hide_index=True)

    st.divider()


# ---------------------------------------------------------------------------
# Empty state
# ---------------------------------------------------------------------------

if not (anomalies or price_changes or new_departures or vanished):
    st.info(
        "Ingen ændringer i vinduet. Det kan skyldes at:\n"
        "  - Du har ikke kørt nye scrapes i perioden\n"
        "  - Markedet er reelt roligt\n"
        "  - Tidsvinduet er for kort\n\n"
        "Prøv at vælge et længere vindue, eller kør en scrape fra Tour-detalje-siden."
    )

    st.divider()
