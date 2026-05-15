# CLAUDE.md — Onboarding for Claude Code / Cowork

Du arbejder på **Topas Travel's prisintelligens-værktøj**. Læs hele dette dokument før du gør noget.

## Hvad værktøjet er

En Streamlit-app der scraper Topas + danske konkurrenter (Albatros, Stjernegaard, Viktors Farmor, Ruby, Smilrejser, Jysk Rejsebureau, Gjøa, Best Travel, Nilles & Gislev, Fyrholt, Kipling, Vagabond) og sammenligner pris + status per afgang. Bruges af head of agency (Gorm, gs@topas.dk) til at se hvornår konkurrenter ændrer pris eller bliver udsolgt.

Topas-segmentet er **fællesrejser med rejseleder** — ikke self-drive, ikke individuel, ikke på-forespørgsel. Når du filtrerer konkurrenter, så filtrér til **fixed-departure guided group tours** med synlige datoer og priser.

## Arkitektur

```
                ┌────────────────────────────────────────┐
                │  GitHub: GsTopas/topas-analyst         │
                │  (kode source of truth)                │
                └──────────────┬─────────────────────────┘
                               │ auto-deploy
                               ▼
            ┌───────────────────────────────────────────┐
            │  Streamlit Cloud — topas-analyst.app      │
            │  (user-facing app, password-gated)        │
            └────────┬──────────────────┬───────────────┘
                     │                  │
                     ▼                  ▼
    ┌────────────────────────┐   ┌────────────────────────┐
    │  Supabase Postgres     │   │  Firecrawl API         │
    │  (data source of truth)│   │  (web scraping)        │
    │  Project bymurhqfcyxdh │   │                        │
    └────────────────────────┘   └────────────────────────┘
                     ▲
                     │ writes scrape- + screening-resultater
                     │
            ┌────────┴──────────────────────────────────┐
            │  topas_scraper.competitor_search          │
            │  Screening: finder kandidat-konkurrenter  │
            │  via Firecrawl Search → Claude Sonnet     │
            │  (tidligere n8n-workflow x6ETjZx9, drop-  │
            │   ped 2026-05-15 efter side-om-side test) │
            └────────────────────────────────────────────┘
```

**Lokal arbejdsmappe:** `C:\Users\gs\Downloads\topas-scraper` (Gorm's clone af repoet). NB: ikke OneDrive-synkroniseret, men file-edits kan stadig blive truncated af antivirus/mount-lag.

## Vigtige services + adgang

| Service | Hvor | Adgang via |
|---|---|---|
| Supabase Postgres | bymurhqfcyxdhrayddoz | MCP `mcp__c38d5069-c478-4ab8-af88-efb21a115057__*` + `.env` SUPABASE_DB_URL |
| Streamlit Cloud | share.streamlit.io | Manage panel — secrets ligger her |
| GitHub repo | GsTopas/topas-analyst | https://github.com/GsTopas/topas-analyst |
| Firecrawl | firecrawl.dev | `.env` FIRECRAWL_API_KEY |
| Anthropic API (vision + screening) | console.anthropic.com | `.env` ANTHROPIC_API_KEY |

## DB-skema (vigtigste tabeller)

Alle ture (Topas + konkurrenter) i samme `tours`-tabel skelnet via `operator`.

- **tours** (operator, tour_slug, tour_name, url, country, region, competes_with, tour_format, duration_days, from_price_dkk, last_seen_run)
- **departures** (operator, tour_slug, start_date, end_date, price_dkk, availability_status, flight_origin, rejseleder_name, last_seen_run) — én række pr. afgang per tur
- **snapshots** (snapshot_id, run_id, operator, tour_slug, start_date, price_dkk, availability_status, observed_at) — historik; bruges af Markeds-kalenderen og Ugentlig-rapport
- **topas_catalog** — Topas's egne 49 fællesrejse-produkter
- **catalog.db tables** (alle i samme Supabase): n8n_candidates, review_decisions, approved_competitor_targets, classification, pattern_observation

`competes_with` = Topas-tour-koden (PTMD, ITTO, ITDA, FRCL, VNSN, etc.) Topas-rækken har `competes_with = tour_code` (sig selv).

## Centrale konventioner

- **Status-enum**: Garanteret, Få pladser, Udsolgt, Afventer pris, Åben. NEVER use andre værdier.
- **Pris-format**: integer DKK uden tusind-separator (12998, ikke 12.998).
- **Datoer**: ISO YYYY-MM-DD i databasen. Dansk DD.MM.YYYY i UI'et via `format_dk_date()`.
- **Nætter→dage**: 7 nætter = 8 dage (konvertering laves i kode, ikke i LLM-prompts).
- **Range-afgange** (fx Gjøa "27. jun → 4. jul"): én departure-række med start_date + end_date. Aldrig to separate.
- **Historiske datoer**: Topas vil have dem bevaret. Markér IKKE past departures som data-quality issues.

## Tier-arkitektur for scraping

Hver scrape kører:
1. **Tier 1**: Firecrawl JSON-extraction via `topas_scraper.extraction_schema` → `generic_ai.parser`. Universalt — virker for alle operatører.
2. **Tier 2 (Albatros)**: Sitemap-discovery i `sitemap_discovery.py` — finder URL-varianter pr. afgang.
3. **Tier 3 (fallback)**: Claude vision i `vision_extractor.py`. Firecrawl screenshot → Sonnet 4.6. Bruges når Tier 1 returnerer 0 eller "thin" (Albatros, Topas, Stjernegaard, Ruby).

Konfig: `topas_scraper/runner.py` orkestrerer; `topas_scraper/config.py` har `PARSER_KEYS_NEEDING_VISION` settet.

## Side-arkitektur i Streamlit

- **Tour-detalje** (pages/2): Hoved-page. Tour-picker, scrape-knap, Markeds-kalender med pris-delta-badges
- **Topas-katalog** (pages/4): Liste over alle Topas-ture
- **Review-kandidater** (pages/5): n8n-screening-output, godkend/afvis konkurrenter
- **Godkendte-konkurrenter** (pages/6): Listen over approved targets
- **Ugentlig-rapport** (pages/7): Bemærkelsesværdige ændringer, pris-fald/-stigninger, nye/fjernede afgange

Alle pages bruger `from topas_scraper._auth import require_auth` til password-gate (kan deaktiveres ved at slette APP_PASSWORD i Streamlit secrets).

## Snapshot-historik + ændringsdetektion

`snapshots` udvides med én række pr. afgang pr. scrape. `export.py` beregner:
- `priceDelta` — pris-ændring vs. ældste snapshot ≥7 dage gammel
- `statusAnomaly` — kategori-skift (withdrawn / status→Udsolgt). Label er "Skiftet til Udsolgt" eller "Trukket fra salg".
- `firstSeen` — ældste snapshot for afgangen (bruges af "Nye afgange"-sektion).
- `isArchived` — true hvis last_seen_run ≠ current run.

Ugentlig-rapport viser disse i 5 sektioner: Bemærkelsesværdige, Pris-fald, Pris-stigninger, Nye afgange, Fjernede afgange.

## Typiske opgaver

**Re-scrape én tour-kode (fx ITTO):**
```python
from topas_scraper.runner import run_scrape_for_tour
run_scrape_for_tour("ITTO")
```

**Generér ændringsrapport:**
```cmd
python -m topas_scraper.weekly_report --out outputs/weekly_report.md
```

**Tjek hvad der ligger i Supabase:**
Brug Supabase MCP `execute_sql` — eller SQL Editor på https://supabase.com/dashboard/project/bymurhqfcyxdhrayddoz/sql

## Vigtige gotchas (lært gennem tid)

1. **OneDrive/mount-lag truncates files**: Edits gennem bash heredoc eller Write-tool kan blive truncated. Verificér altid med `python -c "import ast; ast.parse(open(F,'rb').read())"` efter store edits.
2. **Postgres TEXT-felter til datoer**: `start_date` er TEXT, ikke DATE. Cast eksplicit: `start_date::date`.
3. **PgBouncer Transaction Pooler timeouts**: `_pg_conn.py` har active health-check med `SELECT 1` ping for at fange døde forbindelser.
4. **Streamlit Cloud filesystem er read-only og ephemeral**: Brug `tempfile.gettempdir()` til midlertidige outputs. Persistér via Supabase.
5. **inotify ENOSPC på Streamlit Cloud**: `.streamlit/config.toml` har `fileWatcherType = "none"`.
6. **Streamlit cache invalidation**: efter scrape, kald `_load_dashboard.clear()` så fresh data vises.
7. **Bool subtype af int**: `_safe_int()` ekskluderer bool eksplicit ellers ville True/False blive 1/0.

## Hvem er Gorm

- Head of agency hos Topas Travel
- Email: gs@topas.dk
- Ikke fuld-tids udvikler — vil have ting der virker, ikke perfekte abstraktioner
- Foretrækker CMD-kommandoer med `cd <projekt-path>` først så han kan copy-paste direkte
- Skriver dansk; svar gerne dansk medmindre andet er åbenlyst
