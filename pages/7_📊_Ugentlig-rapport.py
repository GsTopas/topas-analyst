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
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Ugentlig rapport · Topas", page_icon="📊", layout="wide")

st.markdown("# 📊 Ugentlig rapport")
st.caption(
    "Hvad er ændret siden sidst? Sammenligner aktuelle snapshots med tidligere "
    "observationer og fremhæver de prishændelser, status-ændringer og afgangs-bevægelser "
    "der er værd at reagere på."
)

# ---------------------------------------------------------------------------
# Data load
# ---------------------------------------------------------------------------

JSON_PATH = Path("data/dashboard.json")


@st.cache_data(ttl=10)
def _load_dashboard() -> Optional[dict]:
    if not JSON_PATH.exists():
        return None
    try:
        return json.loads(JSON_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


data = _load_dashboard()
if data is None:
    st.warning("Ingen scraped data. Kør `python -m topas_scraper.cli scrape` lokalt først.")
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
m3.metric("🗓️ Forsvundne afgange", len(vanished),
          help="Afgange der ikke længere er på operatørens side, men hvor datoen stadig er fremtidig")
m4.metric("Total fund", len(anomalies) + len(price_changes) + len(vanished))

st.divider()


# ---------------------------------------------------------------------------
# 🚨 Bemærkelsesværdige ændringer (anomalies)
# ---------------------------------------------------------------------------

if anomalies:
    st.markdown("## 🚨 Bemærkelsesværdige ændringer")
    st.caption(
        "Operatører der har trukket afgange fra salg eller hvor afgange er solgt ud "
        "uden at have været i 'Få pladser'-fasen først."
    )
    for a in sorted(anomalies, key=lambda x: x.get("severity", "low") + x.get("changed_at", ""), reverse=True):
        sev_emoji = "🚨" if a["severity"] == "high" else "⚡"
        type_label = {
            "withdrawn": "Trukket fra salg",
            "fast_sellout": "Hurtigt udsolgt",
        }.get(a["type"], a["type"])
        with st.container(border=True):
            cols = st.columns([3, 2, 2, 2])
            with cols[0]:
                st.markdown(f"**{sev_emoji} {a['operator']}** · {a['tour_code']} · {a['tour_name']}")
                start_dt = _parse_iso(a["start_date"])
                st.caption(f"Afgang: **{_format_dk_date(start_dt)}** · {type_label}")
            with cols[1]:
                st.caption("Var")
                st.write(f"{a['previous_state']}")
                if a.get("previous_price"):
                    st.caption(f"{a['previous_price']:,} kr.".replace(",", "."))
            with cols[2]:
                st.caption("Blev til")
                st.write(f"**{a.get('current_state') or '(forsvundet)'}**")
                if a.get("current_price"):
                    st.caption(f"{a['current_price']:,} kr.".replace(",", "."))
            with cols[3]:
                changed_dt = _parse_iso(a["changed_at"])
                st.caption("Ændret")
                st.write(_format_dk_date(changed_dt))

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
# 🗓️ Forsvundne afgange (kun fremtidige)
# ---------------------------------------------------------------------------

if vanished:
    st.markdown(f"## 🗓️ Forsvundne afgange ({len(vanished)})")
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

if not (anomalies or price_changes or vanished):
    st.info(
        "Ingen ændringer i vinduet. Det kan skyldes at:\n"
        "  - Du har ikke kørt nye scrapes i perioden\n"
        "  - Markedet er reelt roligt\n"
        "  - Tidsvinduet er for kort\n\n"
        "Prøv at vælge et længere vindue, eller kør `python -m topas_scraper.cli scrape` "
        "for at hente friske data."
    )
