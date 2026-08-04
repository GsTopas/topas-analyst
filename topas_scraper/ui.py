"""
Fælles UI-hjælpere for Streamlit-siderne.

Samler det der før lå som kopier i de enkelte pages (dashboard-loaderen
fandtes i 2 næsten identiske udgaver med hver sin cache og SAMME delte
temp-fil — to samtidige sessioner kunne læse en halvt skrevet fil).

Én cache her = alle sider ser samme friske data, og én .clear() efter en
scrape invaliderer for hele appen.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Union

import streamlit as st

# Per-proces temp-fil — et fast delt filnavn gav race mellem samtidige
# sessioner (den ene skriver mens den anden læser).
_JSON_PATH = Path(tempfile.gettempdir()) / f"topas_dashboard_{os.getpid()}.json"

MONTHS_DA = ["jan", "feb", "mar", "apr", "maj", "jun",
             "jul", "aug", "sep", "okt", "nov", "dec"]


@st.cache_data(ttl=600, show_spinner=False)
def load_dashboard() -> Optional[dict]:
    """Generér dashboard-payload fra Supabase. Én delt cache for alle sider."""
    try:
        from .export import export as _export  # noqa: PLC0415
        _export(output=_JSON_PATH)
        return json.loads(_JSON_PATH.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        st.error(
            f"⚠ Export fra Supabase fejlede: `{type(exc).__name__}`. "
            "Falder tilbage til lokal `data/dashboard.json` hvis den findes "
            "(kan være gammel) — ellers vises ingen data."
        )
        # Fuld trace kun lokalt — kan lække DSN-fragmenter i prod.
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


def clear_dashboard_cache() -> None:
    """Invalidér dashboard-cachen (kaldes efter scrape)."""
    load_dashboard.clear()


def parse_iso(value: Union[str, date, datetime, None]) -> Optional[date]:
    """ISO-dato/datetime-streng → date. None/ugyldig → None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def format_dk_date(value: Union[str, date, datetime, None]) -> str:
    """→ dansk 'DD.MM.YYYY' (UI-konventionen, jf. CLAUDE.md). Ugyldig → ''."""
    d = parse_iso(value)
    return d.strftime("%d.%m.%Y") if d else ""


def format_dk_date_long(value: Union[str, date, datetime, None]) -> str:
    """→ dansk '13. maj 2026' (aldrig locale-afhængig %b). Ugyldig → ''."""
    d = parse_iso(value)
    if d is None:
        return ""
    return f"{d.day}. {MONTHS_DA[d.month - 1]} {d.year}"


def fmt_dkk(value: Optional[Union[int, float]]) -> str:
    """12998 → '12.998' (dansk tusind-separator). None → '—'."""
    if value is None:
        return "—"
    try:
        return f"{int(value):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "—"
