"""
Hotel-benchmark — Ilulissat-konkurrenternes natpriser pr. kalenderdag.

Kalender-pivot fra hotel_rate_snapshots: én række pr. nat, én kolonne pr.
hotel, celle = billigste tilgængelige dobbeltværelse. Udsolgte nætter
markeres eksplicit. Detalje-sektionen viser alle værelses-offers for en
valgt nat.

Data opdateres via CLI (se hjælpetekst nederst) — sceduleret scrape kommer.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Hotel-benchmark · Topas", page_icon="🏨", layout="wide")

# Password-gate — stopper page-rendering indtil korrekt adgangskode.
from topas_scraper._auth import require_auth  # noqa: E402
require_auth()

from hotel_rates.config import HOTELS, active_hotels  # noqa: E402
from hotel_rates.run import run_rate_scrape_many  # noqa: E402
from topas_scraper._pg_conn import connect  # noqa: E402

st.markdown("# 🏨 Hotel-benchmark — Ilulissat")
st.caption(
    "Konkurrenternes natpriser (dobbeltværelse, 2 pers) pr. kalenderdag, "
    "scrapet direkte fra deres booking-engines. Billigste hotel pr. nat er "
    "markeret. UDSOLGT vises når en motor eksplicit melder alt optaget."
)

UDSOLGT = "Udsolgt"
DISPLAY_NAMES = {k: h.name for k, h in HOTELS.items()}


@st.cache_data(ttl=600)
def _load_rates() -> pd.DataFrame:
    """Seneste komplette snapshot pr. (hotel, nat) fra Supabase.

    VIGTIGT: dedup skal ske pr. RUN, ikke pr. værelsesnavn. Ellers overlever
    gamle pris-rækker når en nat senere skifter til udsolgt (udsolgt-rækken
    har room_name=NULL og er en anden nøgle end pris-rækkerne) — og kalenderen
    ville vise en forældet pris som aktuel. CTE'en finder det nyeste run for
    hver (hotel, nat) og returnerer KUN det runs rækker.

    Kun nætter fra i dag og frem — fortiden bevares i tabellen som historik
    men hører ikke hjemme i en fremadskuende pris-kalender.
    """
    conn = connect()
    cur = conn.execute(
        """
        WITH latest AS (
          SELECT DISTINCT ON (hotel, stay_date) hotel, stay_date, run_id
          FROM hotel_rate_snapshots
          WHERE stay_date >= CURRENT_DATE
          ORDER BY hotel, stay_date, observed_at DESC, id DESC
        )
        SELECT s.hotel, s.stay_date, s.room_name, s.price_dkk,
               s.availability_status, s.observed_at
        FROM hotel_rate_snapshots s
        JOIN latest l
          ON s.hotel = l.hotel AND s.stay_date = l.stay_date
         AND s.run_id = l.run_id
        ORDER BY s.hotel, s.stay_date, s.price_dkk NULLS LAST
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    return pd.DataFrame(rows)


def _fmt_dk_date(d: date) -> str:
    """ISO date → dansk DD.MM.YYYY (UI-konvention, jf. CLAUDE.md)."""
    return d.strftime("%d.%m.%Y")


def _fmt_price(v: object) -> str:
    """2490 → '2.490' (dansk tusind-separator, kun til visning)."""
    if pd.isna(v):
        return "—"
    return f"{int(v):,}".replace(",", ".")


# ---------------------------------------------------------------------------
# Scrape-knap: kør et nyt scrape direkte fra siden (som Tour-detalje).
# Nætterne fordeles på 5 parallelle workers (Firecrawl hobby-plan-grænsen).
# ---------------------------------------------------------------------------

with st.expander("🔄 Opdater data — kør nyt scrape", expanded=False):
    col_start, col_nights, col_info = st.columns([1, 1, 2])
    with col_start:
        scrape_start = st.date_input("Fra dato", value=date.today())
    with col_nights:
        scrape_nights = st.number_input(
            "Nætter", min_value=1, max_value=90, value=14,
            help="Antal nætter frem fra startdatoen",
        )
    n_hotels = len(active_hotels())
    n_calls = n_hotels * int(scrape_nights)
    with col_info:
        st.caption(
            f"{n_hotels} hoteller × {int(scrape_nights)} nætter = "
            f"**{n_calls} Firecrawl-kald** · ca. {max(1, n_calls * 13 // 60 // 5)}-"
            f"{max(2, n_calls * 16 // 60 // 5)} min (5 parallelle)"
        )
    if st.button("Start scrape", type="primary"):
        with st.status(f"Scraper {n_calls} nætter...", expanded=True) as status:
            try:
                run_rate_scrape_many(
                    [h.key for h in active_hotels()],
                    scrape_start,
                    int(scrape_nights),
                    on_progress=st.write,
                )
                status.update(label="Scrape færdig ✓", state="complete", expanded=False)
            except Exception as exc:  # noqa: BLE001
                status.update(label=f"Scrape fejlede: {exc}", state="error")
                st.stop()
        _load_rates.clear()
        st.rerun()

df = _load_rates()
if df.empty:
    st.warning(
        "Ingen hotel-rates i databasen endnu. Kør en scrape med knappen ovenfor."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Kalender-pivot: billigste tilgængelige pris pr. hotel pr. nat.
# En nat er 'Udsolgt' for et hotel hvis nyeste snapshot-sæt KUN indeholder
# udsolgt-rækker (price_dkk IS NULL + status Udsolgt).
# ---------------------------------------------------------------------------

available = df[df["price_dkk"].notna() & (df["availability_status"] != UDSOLGT)]
cheapest = (
    available.groupby(["stay_date", "hotel"])["price_dkk"].min().unstack("hotel")
)

# Udsolgt-detektion: nætter hvor hotellet HAR snapshots men ingen bookbare
# offers. Reindex på BÅDE index og columns — et hotel der er udsolgt alle
# nætter i vinduet har ingen kolonne i `cheapest` og ville ellers forsvinde
# helt fra kalenderen (i stedet for at stå som UDSOLGT). fillna+astype(bool)
# fordi alignment ellers giver NaN-celler, og bool(NaN) er True i Python.
observed = df.groupby(["stay_date", "hotel"]).size().unstack("hotel")
calendar = cheapest.reindex(index=observed.index, columns=observed.columns)
sold_out_mask = (observed.notna() & calendar.isna()).fillna(False).astype(bool)

calendar = calendar.rename(columns=DISPLAY_NAMES)
sold_out_mask = sold_out_mask.rename(columns=DISPLAY_NAMES)

# ---------------------------------------------------------------------------
# Oversigts-metrics
# ---------------------------------------------------------------------------

n_nights = len(calendar)
first_night = calendar.index.min()
last_night = calendar.index.max()
newest = pd.to_datetime(df["observed_at"]).max()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Nætter i vindue", n_nights)
m2.metric("Hoteller", calendar.shape[1])
m3.metric("Periode", f"{_fmt_dk_date(first_night)} – {_fmt_dk_date(last_night)}")
m4.metric("Senest scrapet", newest.strftime("%d.%m.%Y %H:%M"))

# ---------------------------------------------------------------------------
# Kalender-tabellen med billigst-markering + udsolgt
# ---------------------------------------------------------------------------

st.markdown("## Markeds-kalender")


def _cell_text(idx, col: str, v: object) -> str:
    if sold_out_mask.loc[idx].get(col, False):
        return "UDSOLGT"
    return _fmt_price(v)


# Visnings-kopi med UDSOLGT-tekst (Styler's na_rep kan ikke skelne udsolgt
# fra manglende data) — Styler bruges kun til farverne.
display = calendar.copy().astype("object")
for col in display.columns:
    display[col] = [
        _cell_text(idx, col, v) for idx, v in calendar[col].items()
    ]
display.index = pd.Index(
    [_fmt_dk_date(d) for d in calendar.index], name="Nat"
)
sold_out_display = sold_out_mask.copy()
sold_out_display.index = display.index
calendar_display_min = calendar.copy()
calendar_display_min.index = display.index


def _style_display_row(row: pd.Series) -> list[str]:
    numeric = calendar_display_min.loc[row.name]
    row_min = numeric.min() if numeric.notna().any() else None
    styles = []
    for hotel_col in row.index:
        if sold_out_display.loc[row.name].get(hotel_col, False):
            styles.append("background-color:#8b1a1a;color:white;font-weight:bold")
        elif (
            pd.notna(numeric.get(hotel_col))
            and row_min is not None
            and numeric[hotel_col] == row_min
        ):
            styles.append("background-color:#14532d;color:white;font-weight:bold")
        else:
            styles.append("")
    return styles


st.dataframe(
    display.style.apply(_style_display_row, axis=1),
    width="stretch",
)
st.caption(
    "🟩 billigste hotel den nat · 🟥 UDSOLGT = motoren melder alt optaget · "
    "— = ingen data for natten"
)

# ---------------------------------------------------------------------------
# Nat-detalje: alle værelses-offers for en valgt nat
# ---------------------------------------------------------------------------

st.markdown("## Nat-detalje")

night_options = sorted(df["stay_date"].unique())
chosen = st.selectbox(
    "Vælg nat",
    options=night_options,
    format_func=_fmt_dk_date,
)

detail = df[df["stay_date"] == chosen].copy()
detail["hotel"] = detail["hotel"].map(DISPLAY_NAMES).fillna(detail["hotel"])
detail = detail.sort_values(["hotel", "price_dkk"], na_position="last")
detail_view = pd.DataFrame({
    "Hotel": detail["hotel"],
    "Værelse": detail["room_name"].fillna("(ingen ledige)"),
    "Pris/nat": detail["price_dkk"].map(_fmt_price),
    "Status": detail["availability_status"],
    "Observeret": pd.to_datetime(detail["observed_at"]).dt.strftime("%d.%m.%Y %H:%M"),
})
st.dataframe(detail_view, width="stretch", hide_index=True)

# ---------------------------------------------------------------------------
# Hjælp
# ---------------------------------------------------------------------------

with st.expander("Sådan opdaterer du data (CLI)"):
    st.code(
        "cd C:\\Users\\gs\\Downloads\\topas-scraper\n"
        "set PYTHONIOENCODING=utf-8\n"
        "python -m hotel_rates.run --hotel hotel-arctic --start 2026-08-10 --nights 14\n"
        "REM gentag for: hvide-falk · hotel-icefiord · hotel-ilulissat\n"
        "REM (lokalt hos Gorm: brug Anaconda Prompt, eller fuld sti til anaconda3\\python.exe)",
        language="batch",
    )
    st.caption(
        "Hoteller: "
        + " · ".join(f"`{k}` ({h.name})" for k, h in HOTELS.items())
    )
