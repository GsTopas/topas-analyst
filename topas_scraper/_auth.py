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

import os

import streamlit as st


def _get_password() -> str:
    """Hent password fra Streamlit secrets (deployment) eller env (lokalt)."""
    try:
        pwd = st.secrets.get("APP_PASSWORD", "")
        if pwd:
            return pwd
    except Exception:
        pass
    return os.getenv("APP_PASSWORD", "")


def require_auth() -> None:
    """Vis login-form hvis ikke authenticated. Stopper page-eksekvering
    indtil korrekt password indtastes."""
    expected = _get_password()
    if not expected:
        # Hvis password ikke er konfigureret, springer vi auth helt over.
        # Bruges til lokal udvikling hvor APP_PASSWORD ikke er sat.
        return

    if st.session_state.get("authed"):
        return

    # Vis login-form og stop page-rendering indtil korrekt password
    st.markdown("# 🔒 Topas Prisintelligens")
    st.markdown("Indtast adgangskode for at fortsætte.")

    with st.form("login_form"):
        pwd = st.text_input("Adgangskode", type="password", key="auth_pwd_input")
        submitted = st.form_submit_button("Log ind")

    if submitted:
        if pwd == expected:
            st.session_state["authed"] = True
            st.rerun()
        else:
            st.error("Forkert adgangskode.")

    st.stop()
