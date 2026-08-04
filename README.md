# Topas Prisintelligens

Streamlit-app der overvåger Topas Travels konkurrenter: scraper priser og
tilgængelighed for fællesrejser (15 danske operatører) og hotel-natpriser
(Ilulissat-hoteller), og viser sammenligninger, markeds-kalendere og
ændringsrapporter.

**→ Fuld arkitektur, DB-skema, konventioner og gotchas: se [CLAUDE.md](CLAUDE.md)**
(den vedligeholdes som onboarding-dokument og er source of truth).

## Setup

Kræver Python 3.13. Kopiér `.env.example` til `.env` og udfyld nøglerne
(Firecrawl, Anthropic, Supabase — se kommentarerne i filen).

```cmd
pip install -r requirements.txt
```

Udvikling (tests m.m.):

```cmd
pip install -r requirements.txt -r requirements-dev.txt
```

## Kørsel

```cmd
REM Appen lokalt
streamlit run streamlit_app.py

REM Tur-scrape (alle targets, eller én tour-kode)
python -m topas_scraper.cli scrape
python -m topas_scraper.cli scrape --tour PTMD

REM Hotel-rates (Ilulissat-pilot)
python -m hotel_rates.run --hotel hotel-arctic --start 2026-08-10 --nights 14

REM Ugentlig ændringsrapport
python -m topas_scraper.weekly_report --out outputs/weekly_report.md

REM Tests
python -m pytest tests/ -v
```

Alle scrapes startes **manuelt** (CLI, knapper i appen, eller GitHub Actions
→ "Weekly scrape" → *Run workflow*). Der er bevidst ingen automatisk cron så
længe værktøjet ikke er i drift.

## Deployment

Push til `main` → Streamlit Cloud auto-deployer (topas-analyst).
Secrets (inkl. `APP_PASSWORD` og `SUPABASE_DB_URL`) ligger i Streamlit
Cloud's Manage-panel — appen fail-closer hvis `APP_PASSWORD` mangler dér.

Commit med specifikke stier (`git add <filer>`) — aldrig `git add -A`.
