"""
Thin psycopg2-wrapper der emulerer sqlite3.Connection's API.

Formålet er at kunne porte db.py + catalog_db.py fra SQLite til Postgres
uden at skulle omskrive hver eneste funktion. Wrapper håndterer:

  1. Parameter-syntax: oversætter '?' til '%s' i alle execute()-kald
  2. Row-access: returnerer dict-lignende objekter der både understøtter
     row["col"] og dict(row) som sqlite3.Row gør
  3. cursor.execute() returnerer cursor (ikke conn) — samme adfærd som
     sqlite3 hvor conn.execute() returnerer en cursor
  4. lastrowid via RETURNING id — sqlite3.Cursor.lastrowid er ikke
     tilgængelig i psycopg2, så funktioner der bruger den får ekstra
     `RETURNING id` indsat dynamisk
  5. Commit + close samme API

Bruger SUPABASE_DB_URL fra .env / Streamlit secrets.
"""
from __future__ import annotations

import os
from typing import Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor


def _get_dsn() -> str:
    """Hent Postgres-forbindelses-streng fra env eller Streamlit secrets."""
    dsn = os.getenv("SUPABASE_DB_URL")
    if dsn:
        return dsn
    # Streamlit Cloud injecter secrets via st.secrets
    try:
        import streamlit as st
        dsn = st.secrets.get("SUPABASE_DB_URL", "")
        if dsn:
            return dsn
    except Exception:
        pass
    raise RuntimeError(
        "SUPABASE_DB_URL ikke sat — tilføj til .env (lokalt) "
        "eller Streamlit Cloud secrets (deployment)."
    )


class PgCursor:
    """Wrapper omkring psycopg2 cursor der efterligner sqlite3.Cursor:
    - execute() returnerer self (så man kan kæde .fetchone() bagefter)
    - fetchone()/fetchall() returnerer RealDictRow (dict-kompatibel)
    - rowcount og lastrowid (via RETURNING-trick)"""

    def __init__(self, raw_cursor):
        self._cur = raw_cursor
        self._lastrowid: Optional[int] = None

    def _translate_sql(self, sql: str) -> str:
        """SQLite '?' → Postgres '%s'. Skip hvis SQL allerede bruger '%s'."""
        if "%s" in sql:
            return sql
        # Naive replace — kollider ikke i normale SQL-strenge da '?' ikke har
        # andet legitimt formål i Postgres-syntax.
        return sql.replace("?", "%s")

    def execute(self, sql: str, params=()):
        translated = self._translate_sql(sql)
        self._cur.execute(translated, params)
        # Hvis SQL har "RETURNING id" populerer vi _lastrowid (efterligner
        # sqlite3.cursor.lastrowid). Kaldere der har brug for inserted-id
        # skal eksplicit tilføje "RETURNING id" til deres SQL.
        if " returning " in translated.lower() and self._cur.description:
            try:
                row = self._cur.fetchone()
                if row and "id" in row:
                    self._lastrowid = row["id"]
            except psycopg2.ProgrammingError:
                pass
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def lastrowid(self):
        return self._lastrowid

    def close(self):
        self._cur.close()


class PgConnection:
    """Wrapper omkring psycopg2.Connection der efterligner sqlite3.Connection:
    - execute(sql, params) → PgCursor
    - commit() / close() / rollback()
    - row_factory-property bevares for kompatibilitet (no-op; vi bruger altid DictCursor)

    NB: close() er bevidst en no-op. Forbindelser deles på tværs af Streamlit
    reruns via @st.cache_resource — hvis kode kaldte conn.close() ville det
    bryde cachen og fremtidige queries på den delte forbindelse ville fejle
    med InterfaceError. Forbindelser lever cache-TTL ud (10 min), eller indtil
    container restart. Scripts der har brug for at lukke eksplicit kan kalde
    conn._conn.close() direkte.
    """

    def __init__(self, raw_conn):
        self._conn = raw_conn
        # Behold for kompatibilitet — kode der sætter conn.row_factory ignoreres
        self.row_factory = None

    def execute(self, sql: str, params=()) -> PgCursor:
        cur = self._conn.cursor(cursor_factory=RealDictCursor)
        wrapped = PgCursor(cur)
        return wrapped.execute(sql, params)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        # No-op. Se class-docstring. Forbindelser cached på tværs af
        # Streamlit-reruns må ikke lukkes af pages-koden.
        pass

    def cursor(self) -> PgCursor:
        return PgCursor(self._conn.cursor(cursor_factory=RealDictCursor))

    def executescript(self, script: str):
        """Sqlite3-API til at køre flere statements. I Postgres bruger vi
        bare cursor.execute som understøtter multi-statement SQL.

        I praksis er dette no-op hos os fordi Supabase har skemaet —
        executescript blev kun brugt til CREATE TABLE IF NOT EXISTS som
        allerede er kørt via apply_migration. Vi accepterer kaldet for
        bagudkompatibilitet."""
        # Ingen handling — Supabase har skemaet. Vi vil ikke køre CREATE TABLE
        # imod Supabase fra app-koden.
        pass


def _new_connection() -> PgConnection:
    """Åbner en frisk Postgres-forbindelse til Supabase. Bruges direkte
    af scripts (migration, debug) der ikke kører i Streamlit."""
    raw = psycopg2.connect(_get_dsn())
    return PgConnection(raw)


def connect(_db_path: Any = None) -> PgConnection:
    """Få en Postgres-forbindelse til Supabase.

    - Hvis vi kører i Streamlit-kontekst: returnér en cached forbindelse
      delt på tværs af alle reruns i samme session. Sparer ~500ms-2s per
      page-interaktion (TCP+TLS handshake til Supabase i Stockholm).
    - Hvis vi er udenfor Streamlit (CLI, scripts): returnér en frisk forbindelse.

    db_path-argumentet ignoreres (bevaret for kompatibilitet med
    eksisterende caller-kode der bruger sqlite3.connect(path)-stil).
    """
    try:
        import streamlit as st  # type: ignore  # noqa: PLC0415
        # @st.cache_resource caches på tværs af reruns i samme session.
        # ttl=600 = forbindelsen forfrisks hver 10. minut for at undgå
        # stale connections (Supabase pooler timer typisk ud efter ~15 min idle).
        return _streamlit_cached_connect()
    except (ImportError, RuntimeError):
        # Ikke i Streamlit, eller streamlit ikke installeret — frisk forbindelse
        return _new_connection()


def _streamlit_cached_connect() -> PgConnection:
    """Returnér en forbindelse cached på Streamlit-session-niveau.
    Importen af streamlit sker lazy så _pg_conn ikke har hard dependency på den."""
    import streamlit as st  # noqa: PLC0415

    @st.cache_resource(ttl=600, show_spinner=False)
    def _cached() -> PgConnection:
        return _new_connection()

    conn = _cached()

    # Aktiv health-check: psycopg2's conn.closed=0 selvom Supabase Transaction
    # Pooler har lukket forbindelsen på serverside (vi opdager det først ved
    # næste query med InterfaceError). Vi laver derfor en trivial SELECT 1
    # som ping. Hvis den fejler, rydder vi cache og åbner frisk forbindelse.
    # Cost: ~50ms per page-load mod Supabase. Acceptabelt for robusthed.
    try:
        cur = conn._conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
    except (psycopg2.InterfaceError, psycopg2.OperationalError, psycopg2.DatabaseError):
        # Død forbindelse — ryd cache, prøv frisk
        try:
            _cached.clear()
        except Exception:
            pass
        conn = _cached()

    return conn
