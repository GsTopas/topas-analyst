"""
Godkendte konkurrent-ture — fase 2.5

Viser hvad scraper'en faktisk vil hente når du trykker "Scrape PTMD" i
Tour-detalje. Per række:
  - Operatør, tur-navn, URL, antal dage, parser-key
  - Hvornår godkendt, af hvem, beslutnings-id
  - Slet-knap (fjerner fra scraper-listen, men ikke fra decision-log)

Sammenhænge i systemet:
  Review-kandidater (Godkend) → approved_competitor_targets ← denne side
                                       ↓
                                "Scrape PTMD" i Tour-detalje
                                       ↓
                                snapshots.db (ægte afgange)
                                       ↓
                                Tour-detalje viser data
"""
from __future__ import annotations

import os
from collections import defaultdict
from typing import Any

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Godkendte konkurrenter · Topas",
    page_icon="✅",
    layout="wide",
)

# Password-gate — stopper page-rendering indtil korrekt adgangskode.
from topas_scraper._auth import require_auth  # noqa: E402
require_auth()


from topas_scraper import catalog_db
from topas_scraper.db import connect as connect_snapshots, fetch_topas_catalog

import re

def _auto_categorize(operator: str, tour_name: str, tour_url: str) -> str | None:
    """Derive category from name + URL using simple heuristics. None if ambiguous."""
    name = (tour_name or "").lower()
    url = (tour_url or "").lower()
    text = f"{name} {url}"
    has_cykel = bool(re.search(r"cykel|cykling|cykle|bike|bicycl|cycling", text))
    has_vandre = bool(re.search(r"vandr|hike|trek|trail", text))
    has_kultur = bool(re.search(r"kultur|rundrejse|all.?inclusive|all.?incl", text))
    if has_cykel and has_vandre:
        return "kombineret"
    if has_cykel:
        return "cykel"
    if has_vandre and has_kultur:
        return "kombineret"
    if has_vandre:
        return "vandre"
    if has_kultur:
        return "kultur"
    return None



def _conn():
    return catalog_db.connect()


def _topas_tour_lookup() -> dict[str, dict[str, Any]]:
    snap = connect_snapshots()
    rows = fetch_topas_catalog(snap)
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        d = dict(r)
        code = d.get("tour_code")
        if code:
            out[code] = d
    return out


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("✅ Godkendte konkurrent-ture")
st.caption(
    "Konkurrenter der er **godkendt via review** og dermed aktive i scraper'en. "
    "Når du trykker 'Scrape PTMD' i Tour-detalje, hentes ægte afgange og priser "
    "fra disse URLs via operatør-specifikke parsers."
)

conn = _conn()
all_targets = catalog_db.list_approved_targets(conn)
topas_tours = _topas_tour_lookup()

if not all_targets:
    st.info(
        "Ingen godkendte konkurrent-ture endnu. Gå til **📋 Review-kandidater**, "
        "godkend ture du vil tracke, og de dukker op her."
    )
    st.stop()

# Group by topas_tour_code
groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
for t in all_targets:
    groups[t["topas_tour_code"]].append(t)

col_metric, col_btn = st.columns([1, 2])
with col_metric:
    st.metric("Godkendte ture i alt", len(all_targets))
with col_btn:
    st.write("")
    if st.button("✨ Auto-kategorisér tomme", help="Foreslår kategori for ture uden kategori baseret på URL + navn"):
        updated = 0
        for t in all_targets:
            if t.get("tour_category"):
                continue
            cat = _auto_categorize(t["operator"], t.get("tour_name") or "", t.get("tour_url") or "")
            if cat:
                catalog_db.update_approved_target_category(conn, target_id=t["id"], tour_category=cat)
                updated += 1
        if updated:
            st.success(f"Kategoriserede {updated} ture automatisk. De øvrige skal sættes manuelt.")
            st.rerun()
        else:
            st.info("Ingen ture at kategorisere — alle har enten en kategori eller falder uden for heuristikkerne.")
st.divider()

# ---------------------------------------------------------------------------
# Per-Topas-tour sections
# ---------------------------------------------------------------------------

# Sort tour codes by tour-name from catalog (fallback to code)
def _sort_key(code: str) -> str:
    name = topas_tours.get(code, {}).get("tour_name") or code
    return name.lower()


sorted_codes = sorted(groups.keys(), key=_sort_key)

for code in sorted_codes:
    rows = groups[code]
    catalog_match = topas_tours.get(code, {})
    tour_name = catalog_match.get("tour_name") or "(ukendt)"
    country = catalog_match.get("country") or "—"

    st.markdown(f"### {code} — {tour_name}")
    st.caption(f"Land: {country} · {len(rows)} godkendte konkurrent-ture")

    for t in rows:
        with st.container(border=True):
            head_cols = st.columns([3, 1, 1, 1])
            with head_cols[0]:
                st.markdown(f"**{t['operator']}** — {t.get('tour_name') or '(ingen tur-navn)'}")
                if t.get("tour_url"):
                    st.markdown(f"[{t['tour_url']}]({t['tour_url']})")
            with head_cols[1]:
                dur = t.get("duration_days")
                st.caption("**Dage**")
                st.write(f"{dur} dage" if dur else "—")
            with head_cols[2]:
                st.caption("**Kategori**")
                category_options = ["", "vandre", "cykel", "kultur", "kombineret", "andet"]
                current_cat = (t.get("tour_category") or "").lower()
                if current_cat not in category_options:
                    current_cat = ""
                new_cat = st.selectbox(
                    "Kategori",
                    options=category_options,
                    index=category_options.index(current_cat),
                    format_func=lambda v: {
                        "": "— sæt kategori —",
                        "vandre": "🥾 Vandre",
                        "cykel": "🚴 Cykel",
                        "kultur": "🏛️ Kultur",
                        "kombineret": "🔀 Kombineret",
                        "andet": "❓ Andet",
                    }[v],
                    key=f"cat_{t['id']}",
                    label_visibility="collapsed",
                )
                if new_cat != current_cat:
                    catalog_db.update_approved_target_category(
                        conn, target_id=t["id"], tour_category=new_cat or None,
                    )
                    st.rerun()
            with head_cols[3]:
                st.caption("**Godkendt**")
                approved_at = t.get("approved_at") or ""
                st.write(approved_at[:10] if approved_at else "—")

            meta_cols = st.columns([3, 1])
            with meta_cols[0]:
                approver = t.get("approved_by") or "—"
                decision_id = t.get("decision_id") or "—"
                st.caption(
                    f"Godkendt af: **{approver}** · "
                    f"Decision-id: `{decision_id}`"
                )
            with meta_cols[1]:
                if st.button(
                    "🗑️ Fjern fra scraper",
                    key=f"del_{t['id']}",
                    help="Fjerner kun fra scraper-listen. Decision-loggen er bevaret som audit-trail.",
                ):
                    removed = catalog_db.delete_approved_target(
                        conn,
                        operator=t["operator"],
                        tour_url=t["tour_url"],
                        topas_tour_code=t["topas_tour_code"],
                    )
                    if removed:
                        # Log a manual revoke as a new override decision so audit-trail captures it
                        reviewer = (
                            os.getenv("USER")
                            or os.getenv("USERNAME")
                            or "streamlit"
                        )
                        try:
                            catalog_db.log_review_decision(
                                conn,
                                target_kind="approved_target",
                                target_id=t["id"],
                                action="reject",
                                reason=f"Manuel fjernelse fra scraper-listen ({t['operator']} · {t.get('tour_name') or t['tour_url']})",
                                reviewer=reviewer,
                            )
                        except ValueError:
                            pass
                        st.success(
                            f"Fjernet **{t['operator']} · {t.get('tour_name') or t['tour_url']}** fra scraper."
                        )
                        st.rerun()
                    else:
                        st.warning("Allerede fjernet eller ikke fundet.")
    st.divider()
