"""
Review-kandidater — fase 2 af konkurrent-screening

Streamlit-side hvor head of agency gennemgår n8n's screening-output:
- Henter rækker fra n8n's "Competitor Analysis"-tabel via fetch-webhook'et
- Viser dem grupperet pr. Topas-tur (PTMD, etc.)
- Per kandidat: Godkend / Afvis (med årsag) / Re-screen / Spring over
- Beslutninger gemmes i catalog.db's review_decisions med
  target_kind='n8n_candidate' — samme decision-log som klassificering og match
  bruger, så pattern-synthesis senere kan bruge dem til playbook-regler

Workflow:
1. Tryk "Hent fra n8n" → pull alle rækker fra webhook → upsert i lokal cache
2. Vælg Topas-tur fra dropdown (kun ture med kandidater vises)
3. Gennemgå unreviewed kandidater
4. Beslutninger persisterer; reviewed candidates kan vises samlet nederst
"""
from __future__ import annotations

import json
import os
from typing import Any

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Review-kandidater · Topas",
    page_icon="📋",
    layout="wide",
)

# Password-gate — stopper page-rendering indtil korrekt adgangskode.
from topas_scraper._auth import require_auth  # noqa: E402
require_auth()


from topas_scraper import catalog_db, n8n_client
from topas_scraper.db import connect as connect_snapshots, fetch_topas_catalog


REJECT_REASONS = {
    "wrong_geo": "Forkert geografi (anden region/land)",
    "wrong_activity": "Forkert aktivitet (fx kultur fremfor vandring/cykling)",
    "wrong_duration": "Forkert rejse-længde (for kort/lang vs. Topas-tur)",
    "wrong_format": "Forkert format (selvkør, individuel, forespørgsel)",
    "no_data": "Manglende data (pris/dato) — re-screen",
    "duplicate": "Allerede i kataloget",
    "out_of_scope": "Ikke en konkurrent for Topas",
    "other": "Andet (skriv i note)",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _catalog_conn():
    """Open a fresh connection each rerun — Streamlit reruns may switch threads,
    and SQLite connections are not portable across threads. Connections are
    cheap; no need to cache."""
    return catalog_db.connect()


def _topas_tour_lookup() -> dict[str, dict[str, Any]]:
    """Map tour_code -> {tour_name, country, ...} from snapshots.db's topas_catalog."""
    snap = connect_snapshots()
    rows = fetch_topas_catalog(snap)
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        d = dict(r)
        code = d.get("tour_code")
        if code:
            out[code] = d
    return out


def _confidence_badge(conf: str) -> str:
    if conf == "high":
        return "🟢 high"
    if conf == "medium":
        return "🟡 medium"
    if conf == "low":
        return "🔴 low"
    return "—"


def _bool_badge(v) -> str:
    if v == 1 or v is True:
        return "✅ Ja"
    if v == 0 or v is False:
        return "❌ Nej"
    return "❓ Tjek selv"


def _decision_badge(action: str | None) -> str:
    if action == "approve":
        return "✅ Godkendt"
    if action == "reject":
        return "❌ Afvist"
    if action == "override":
        return "✏️ Override"
    return "⏳ Pending"


def _parse_departures(raw: str | None) -> list[dict[str, Any]]:
    """Best-effort parse of departures_json. Returns [] on any error."""
    if not raw:
        return []
    try:
        deps = json.loads(raw)
        if not isinstance(deps, list):
            return []
        return [d for d in deps if isinstance(d, dict)]
    except (ValueError, TypeError):
        return []


# ---------------------------------------------------------------------------
# Header + sync
# ---------------------------------------------------------------------------

st.title("📋 Review-kandidater")
st.caption(
    "Gennemgå konkurrent-kandidater fundet af n8n's screening-flow. "
    "Beslutninger gemmes i decision-loggen og fodrer pattern-synthesis "
    "(playbook-reglerne) over tid."
)

conn = _catalog_conn()

col_sync, col_status = st.columns([2, 1])

with col_sync:
    if st.button("🔄 Hent fra n8n", type="primary"):
        with st.spinner("Henter rækker fra n8n's Competitor Analysis-tabel..."):
            try:
                rows = n8n_client.fetch_candidates()
            except n8n_client.N8nFetchError as exc:
                st.error(f"Kunne ikke hente fra n8n: {exc}")
                rows = None

        if rows is not None:
            try:
                new_count, processed = catalog_db.upsert_n8n_candidates_bulk(conn, rows)
                st.success(
                    f"Hentede **{len(rows)}** rækker fra n8n · "
                    f"processeret **{processed}** · heraf **{new_count}** nye."
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"Bulk-import fejlede: {exc}")

with col_status:
    show_historical = st.toggle(
        "Vis ældre screeninger",
        value=False,
        help="Som standard vises kun den seneste screening pr. konkurrent-tur. "
        "Slå til for at se alle historiske kørsler (kan vise hallucinationer fra ældre prompts).",
    )
    if show_historical:
        tour_summary = catalog_db.list_n8n_tour_codes(conn)
    else:
        tour_summary = catalog_db.list_latest_n8n_tour_codes(conn)
    if tour_summary:
        total_unrev = sum(t["unreviewed"] for t in tour_summary)
        total = sum(t["total"] for t in tour_summary)
        st.metric("Pending review", f"{total_unrev}/{total}")
    else:
        st.info("Ingen kandidater endnu — tryk **Hent fra n8n**.")

st.divider()

# ---------------------------------------------------------------------------
# Tour picker
# ---------------------------------------------------------------------------

if not tour_summary:
    st.stop()

topas_tours = _topas_tour_lookup()


def _tour_label(t: dict[str, Any]) -> str:
    code = t["tour_code"]
    catalog_match = topas_tours.get(code, {})
    name = catalog_match.get("tour_name") or "(ukendt navn)"
    badge = f"({t['unreviewed']} pending)" if t["unreviewed"] else "(alle reviewed)"
    return f"{code} · {name} {badge}"


tour_codes = [t["tour_code"] for t in tour_summary]
tour_labels = {t["tour_code"]: _tour_label(t) for t in tour_summary}

selected_code = st.selectbox(
    "Vælg Topas-tur at reviewe",
    options=tour_codes,
    format_func=lambda c: tour_labels[c],
)

if not selected_code:
    st.stop()

# ---------------------------------------------------------------------------
# Candidates for selected tour
# ---------------------------------------------------------------------------

show_reviewed = st.toggle("Vis allerede reviewede", value=False)

list_fn = (
    catalog_db.list_n8n_candidates_for_tour
    if show_historical
    else catalog_db.list_latest_n8n_candidates_for_tour
)
candidates = list_fn(
    conn,
    selected_code,
    only_unreviewed=not show_reviewed,
)

if not candidates:
    st.info("Ingen kandidater at vise for denne tur.")
    st.stop()

st.markdown(f"### {len(candidates)} kandidater")

# Ribbon-summary by confidence
high = sum(1 for c in candidates if c["match_confidence"] == "high")
med = sum(1 for c in candidates if c["match_confidence"] == "medium")
low = sum(1 for c in candidates if c["match_confidence"] == "low")
no_match = sum(1 for c in candidates if not c["has_match"])
c1, c2, c3, c4 = st.columns(4)
c1.metric("🟢 High", high)
c2.metric("🟡 Medium", med)
c3.metric("🔴 Low", low)
c4.metric("⚪ No match", no_match)

st.divider()

# ---------------------------------------------------------------------------
# Per-candidate review UI
# ---------------------------------------------------------------------------

# Bulk-fetch alle decisions for de viste kandidater i ÉN query
# (erstatter N+1-mønster med ~14 queries → 2 queries totalt).
_bulk_decisions = catalog_db.bulk_get_n8n_candidate_decisions(conn, candidates)

for cand in candidates:
    n8n_id = cand["n8n_row_id"]
    decision = _bulk_decisions.get(n8n_id)
    decision_action = decision["action"] if decision else None

    with st.container(border=True):
        head_cols = st.columns([3, 1, 1, 1])
        with head_cols[0]:
            domain = cand["competitor_domain"] or "(ukendt)"
            tour_name = cand["tour_name"] or "(ingen tur-navn)"
            st.markdown(f"**{domain}** — {tour_name}")
            if cand["tour_url"]:
                st.markdown(f"[{cand['tour_url']}]({cand['tour_url']})")
        with head_cols[1]:
            st.write(_confidence_badge(cand["match_confidence"]))
        with head_cols[2]:
            dur = cand["duration_days"]
            st.write(f"{dur} dage" if dur else "—")
        with head_cols[3]:
            st.write(_decision_badge(decision_action))

        info_cols = st.columns([1, 1, 1, 2])
        info_cols[0].caption("**Region:**")
        info_cols[0].write(
            f"{cand['search_country'] or '—'} / {cand['search_region'] or '—'}"
        )
        info_cols[1].caption("**Dansk turleder:**")
        info_cols[1].write(_bool_badge(cand.get("has_guide")))
        info_cols[2].caption("**Faste afgange:**")
        info_cols[2].write(_bool_badge(cand.get("has_fixed_departures")))
        info_cols[3].caption("**Notes (fra AI):**")
        info_cols[3].write(cand["notes"] or "—")

        st.caption(
            "Konkret afgangs-data og priser hentes via lokal scraper efter godkendelse."
        )

        if decision:
            with st.expander(
                f"Tidligere beslutning · {decision['decided_at'][:10]} · "
                f"{decision['action']} ({decision.get('reason') or ''})"
            ):
                st.write(decision)
        else:
            # Action form — only if no prior decision
            with st.form(key=f"form_{n8n_id}", clear_on_submit=False):
                action_col, reason_col, note_col = st.columns([1, 2, 3])
                with action_col:
                    action = st.radio(
                        "Beslutning",
                        options=["approve", "reject", "rescreen"],
                        format_func=lambda a: {
                            "approve": "✅ Godkend",
                            "reject": "❌ Afvis",
                            "rescreen": "🔁 Re-screen",
                        }[a],
                        key=f"action_{n8n_id}",
                    )
                with reason_col:
                    reason_keys = st.multiselect(
                        "Årsag(er)",
                        options=list(REJECT_REASONS.keys()),
                        format_func=lambda k: REJECT_REASONS[k],
                        key=f"reasons_{n8n_id}",
                        help="Vælg en eller flere. Påkrævet ved Afvis. Valgfri ved Re-screen og Godkend.",
                    )
                with note_col:
                    note = st.text_input(
                        "Note (valgfri ved Godkend, anbefalet ved Afvis)",
                        key=f"note_{n8n_id}",
                    )
                submitted = st.form_submit_button("Gem beslutning")
                if submitted:
                    note_text = note.strip()
                    reasons_combined = " + ".join(REJECT_REASONS[k] for k in reason_keys)

                    # Build the human-readable reason string per action
                    if action == "reject":
                        if not reasons_combined and not note_text:
                            st.error("Vælg mindst én årsag eller skriv en note ved Afvis.")
                            st.stop()
                        full_reason = reasons_combined
                        if note_text:
                            full_reason = (
                                f"{reasons_combined} — {note_text}"
                                if reasons_combined else note_text
                            )
                    elif action == "rescreen":
                        rescreen_base = reasons_combined or "Re-screen requested"
                        full_reason = (
                            f"{rescreen_base} — {note_text}" if note_text else rescreen_base
                        )
                    else:  # approve
                        if reasons_combined and note_text:
                            full_reason = f"{reasons_combined} — {note_text}"
                        else:
                            full_reason = reasons_combined or note_text or "Godkendt uden note"

                    db_action = (
                        "approve" if action == "approve"
                        else ("reject" if action == "reject" else "override")
                    )
                    override_payload: dict = {}
                    if reason_keys:
                        override_payload["reason_keys"] = reason_keys
                    if action == "rescreen":
                        override_payload["request"] = "rescreen"
                    reviewer = os.getenv("USER") or os.getenv("USERNAME") or "streamlit"

                    decision_id = catalog_db.log_review_decision(
                        conn,
                        target_kind="n8n_candidate",
                        target_id=n8n_id,
                        action=db_action,
                        reason=full_reason,
                        override_payload=override_payload or None,
                        reviewer=reviewer,
                    )

                    # Auto-promote / auto-demote scraper-target based on decision
                    operator = cand.get("competitor_domain") or ""
                    tour_url = cand.get("tour_url") or ""
                    topas_code = cand.get("topas_tour_code") or ""
                    if operator and tour_url and topas_code:
                        if db_action == "approve":
                            newly_added = catalog_db.upsert_approved_target(
                                conn,
                                operator=operator,
                                tour_url=tour_url,
                                topas_tour_code=topas_code,
                                tour_name=cand.get("tour_name"),
                                duration_days=cand.get("duration_days"),
                                tour_category=cand.get("tour_category"),
                                approved_by=reviewer,
                                decision_id=decision_id,
                            )
                            promote_msg = (
                                "→ tilføjet til scraper" if newly_added
                                else "→ allerede i scraper, opdateret"
                            )
                            st.success(f"Beslutning gemt for kandidat #{n8n_id}. {promote_msg}")
                        elif db_action == "reject":
                            removed = catalog_db.delete_approved_target(
                                conn, operator, tour_url, topas_code,
                            )
                            removal_msg = (
                                "→ fjernet fra scraper" if removed
                                else ""
                            )
                            st.success(f"Beslutning gemt for kandidat #{n8n_id}. {removal_msg}".strip())
                        else:
                            st.success(f"Beslutning gemt for kandidat #{n8n_id}.")
                    else:
                        st.success(f"Beslutning gemt for kandidat #{n8n_id}.")
                    st.rerun()
