"""
Simpel password-gate for Streamlit-appen.

Single shared password — gemt i Streamlit Cloud secrets (eller .env lokalt).
Ingen brugerkonti, ingen hashing — det er en intern tool, ikke en SaaS.

Usage i hver page-fil (inklusive streamlit_app.py):
    from topas_scraper._auth import require_auth
    require_auth()
    # ... resten af page-koden

require_auth() viser en login-form og kalder st.stop() hvis ikke
authenticated. Når password matcher, sættes session_state["authed"] = True
og resten af page-koden eksekveres.
"""
from __future__ import annotations

import hmac
import os
import time

import streamlit as st


# Rate-limit konstanter
_LOCKOUT_AFTER_ATTEMPTS = 5
_LOCKOUT_SECONDS = 30


def _get_password() -> str:
    """Hent password fra Streamlit secrets (deployment) eller env (lokalt)."""
    try:
        pwd = st.secrets.get("APP_PASSWORD", "")
        if pwd:
            return pwd
    except Exception:
        pass
    return os.getenv("APP_PASSWORD", "")


def _is_streamlit_cloud() -> bool:
    """Streamlit Cloud sætter STREAMLIT_RUNTIME_ENV i container."""
    return bool(os.getenv("STREAMLIT_RUNTIME_ENV")) or bool(os.getenv("STREAMLIT_SHARING_MODE"))


def require_auth() -> None:
    """Vis login-form hvis ikke authenticated. Stopper page-eksekvering
    indtil korrekt password indtastes."""
    expected = _get_password()
    if not expected:
        # Fail-closed på Streamlit Cloud: en manglende APP_PASSWORD i secrets
        # er en deploy-fejl, ikke et "spring auth over"-signal.
        if _is_streamlit_cloud():
            st.error(
                "🔒 APP_PASSWORD er ikke konfigureret i Streamlit secrets. "
                "Appen kan ikke startes uden adgangskode i denne deployment."
            )
            st.stop()
        # Lokalt: tillad uden auth (dev-convenience).
        return

    if st.session_state.get("authed"):
        return

    # Vis login-form og stop page-rendering indtil korrekt password
    st.markdown("# 🔒 Topas Prisintelligens")
    st.markdown("Indtast adgangskode for at fortsætte.")

    # Rate-limit: efter N forsøg, lock i 30 sekunder. Per session_state, så
    # browser-cookie/incognito kan bypasse — men det er stadig en effektiv
    # speedbump mod naivt bruteforce.
    locked_until = st.session_state.get("auth_locked_until", 0.0)
    now = time.time()
    if locked_until > now:
        remaining = int(locked_until - now)
        st.error(f"For mange forkerte forsøg. Vent {remaining} sek. og prøv igen.")
        st.stop()

    with st.form("login_form"):
        pwd = st.text_input("Adgangskode", type="password", key="auth_pwd_input")
        submitted = st.form_submit_button("Log ind")

    if submitted:
        if hmac.compare_digest(pwd, expected):
            st.session_state["authed"] = True
            st.session_state.pop("auth_fail_count", None)
            st.session_state.pop("auth_locked_until", None)
            st.rerun()
        else:
            fails = st.session_state.get("auth_fail_count", 0) + 1
            st.session_state["auth_fail_count"] = fails
            if fails >= _LOCKOUT_AFTER_ATTEMPTS:
                st.session_state["auth_locked_until"] = now + _LOCKOUT_SECONDS
                st.session_state["auth_fail_count"] = 0
                st.error(f"For mange forkerte forsøg. Låst i {_LOCKOUT_SECONDS} sek.")
            else:
                st.error(f"Forkert adgangskode ({fails}/{_LOCKOUT_AFTER_ATTEMPTS}).")

    st.stop()
