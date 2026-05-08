# Topas Pricing Scraper

Konkurrencedygtig prisintelligens for Topas Travel. **Streamlit-app** der scraper Topas + danske konkurrenter, sammenligner per-departure-niveau, og tillader live re-scraping pr. tour.

## Status

**v1 — produktionsklar** (deploy nu):
- Streamlit-app med 3 sider: Tour-overblik, Tour-detalje, Konkurrent-overblik
- Live "scrape now"-knap pr. tour
- 6 Madeira-operatører scrapet (5 eligible + 1 ineligible)
- Methodologi-flag (slope-mismatch, competitor-sellout) computeres automatisk

**v2 — fundament klar, ikke testet** (næste session):
- AI-classification-lag bygget (catalog_db, discovery, classifier)
- Klassificerings-playbook med feedback-loop indbygget
- Anthropic-integration klar
- Mangler: reviewer-UI, end-to-end test, integration

## Hvad der ligger i denne bundle

```
topas-scraper/
├── streamlit_app.py            ← v1: Hovedindgang
├── pages/
│   ├── 1_📊_Tour-overblik.py    ← v1: Tabel-oversigt
│   ├── 2_🔍_Tour-detalje.py     ← v1: Detalje + konkurrent-picker + LIVE SCRAPE
│   └── 3_🏢_Konkurrent-overblik.py ← v1
├── topas_scraper/
│   ├── cli.py                  ← v1: CLI scrape command
│   ├── runner.py               ← v1: Reusable scrape (used by Streamlit + CLI)
│   ├── config.py               ← v1: URLs + tour-koder
│   ├── client.py               ← v1: Firecrawl-wrapper
│   ├── db.py                   ← v1: SQLite snapshots schema
│   ├── export.py               ← v1: dashboard.json export
│   ├── parsers/                ← v1: Per-operator-parsere
│   ├── catalog_db.py           ← v2: AI-katalog DB-skema (UTESTET)
│   ├── discovery.py            ← v2: Tour-URL discovery (UTESTET)
│   └── classifier.py           ← v2: Claude API-klassificering (UTESTET)
├── classification_playbook.md  ← v2: AI's regler (vokser over tid)
├── data/
│   └── .gitkeep
├── tests/test_parsers.py       ← v1: Parser smoke-tests
├── .streamlit/config.toml      ← v1: Theme
├── requirements.txt            ← v1+v2 deps (anthropic er ekstra for v2)
├── .env.example
└── README.md (denne fil)
```

**Vigtig observation:** v2-filerne er bygget men ikke testet. De er på plads klar til næste session, men du skal ikke bruge dem endnu. Behold dem i mappen — de virker ikke i vejen for v1.

---

## Hvad du gør nu

Følg disse trin i rækkefølge. Spring ikke fremad.

### Trin 1 — Test Streamlit-app'en lokalt (15 min)

I Git Bash:

```bash
cd /c/Users/gs/Downloads/topas-scraper/topas-scraper
pip install -r requirements.txt
```

Sæt din Firecrawl-nøgle op:

```bash
cp .env.example .env
notepad .env
```

I notepad, sæt `FIRECRAWL_API_KEY=fc-din-nøgle-her`. Gem og luk.

Start app'en:

```bash
streamlit run streamlit_app.py
```

Browser åbner på `http://localhost:8501`.

**Verificer:**
- Forsiden viser snapshot-meta (eller "Ingen scraped data endnu")
- Klik **🔍 Tour-detalje** i sidebaren
- Hvis der ikke er data: klik **🔄 Live scrape**, vent 60 sek, observer status-output
- Konkurrent-vælger nederst skifter mellem Smilrejser, Jysk, Viktors Farmor, Ruby
- Per-departure tabel viser realt sammenligning

Hvis det virker lokalt, kører det også på Streamlit Cloud.

### Trin 2 — Push til GitHub

I Git Bash:

```bash
cd /c/Users/gs/Downloads/topas-scraper/topas-scraper
git add -A
git commit -m "Switch to Streamlit + AI layer foundation"
git push
```

### Trin 3 — Deploy til Streamlit Cloud

1. Gå til https://share.streamlit.io
2. Log ind med GitHub
3. Klik "New app"
4. Repo: `GsTopas/topas-analyst`
5. Branch: `main`
6. Main file path: `streamlit_app.py`
7. Klik "Deploy"

App'en bygger ~2-3 minutter første gang. Du får en URL som
`https://gstopas-topas-analyst-streamlit-app-xxxxxx.streamlit.app`.

### Trin 4 — Tilføj Firecrawl-nøgle som Streamlit-secret

1. På Streamlit-dashboardet, klik "Manage app" nederst til højre
2. "Settings" → "Secrets"
3. Tilføj:
   ```toml
   FIRECRAWL_API_KEY = "fc-din-nøgle-her"
   ```
4. Save. App'en restarter automatisk.

### Trin 5 — Verificer live-scrape virker på Cloud

Klik "🔄 Live scrape" på Tour-detalje siden. Det er lakmustesten — hvis det virker, er v1 deployed og produktionsklar.

---

## Hvad er anderledes ift. den gamle GitHub Pages-løsning

| Område | Før (GitHub Pages) | Nu (Streamlit) |
|---|---|---|
| Hosting | GitHub Pages | Streamlit Community Cloud |
| Auto-deploy | GitHub Actions weekly cron | Streamlit redeployer ved hver git push |
| Live-scrape | Ikke muligt (statisk HTML) | **Knap pr. tour, server-side Python** |
| Auth | Password-gate (klient-side) | Public for nu, auth tilføjes senere |
| Visuelt look | Tilpasset Fraunces + custom CSS | Streamlits standardlook |

Den gamle GitHub Pages-app virker stadig på `gstopas.github.io/topas-analyst/`, men er ikke længere automatisk opdateret. Du kan slette den fra repo-settings hvis du vil — eller lade den stå.

---

## Hvor v2-koden er klar (ikke aktiv)

Hvis du er nysgerrig:

- **`catalog_db.py`** — Database-skema med discovery + classification + feedback-loop tabeller
- **`classification_playbook.md`** — Skriftlige regler AI vil bruge til klassificering. Indholdet er det vi har lært om operatørerne hidtil.
- **`discovery.py`** — Modul der finder tour-URLs på en konkurrents site via sitemap eller Firecrawl /map
- **`classifier.py`** — Modul der bruger Claude API til at vurdere om en tour er Fællesrejse-eligible, hvilken aktivitet, difficulty osv.

Ingen af disse bliver kaldt endnu. De ligger klar til Session 2.

---

## Næste session — hvad vi bygger

Med v1 deployed og verificeret, bygger vi:

1. **Reviewer-UI** — Streamlit-side hvor du gennemgår AI's klassificeringer og overruler manuelt
2. **Pipeline-orchestrator** — Knytter discovery → extract → classify → review sammen
3. **End-to-end test** — Vi kører hele AI-flowet mod én ny konkurrent (Albatros) og verificerer
4. **Pattern-synthesis** — "Find mønstre"-knappen der læser dine review-beslutninger og foreslår nye playbook-regler

Nær-på estimat: 2-3 sessioner mere før AI-laget er produktionsklart for alle 10 konkurrenter.

---

## CLI virker stadig

For at trigger en scrape uden Streamlit (fx fra cron eller debugging):

```bash
python -m topas_scraper.cli scrape           # alle targets
python -m topas_scraper.cli scrape --tour PTMD  # kun PTMD og dens konkurrenter
python -m topas_scraper.cli report           # print summary af seneste run
python -m topas_scraper.cli diff             # compare seneste vs forrige run
```

---

## Fejlfinding

**`streamlit run` fejler med "module not found"**
→ `pip install -r requirements.txt` igen. Sandsynligvis manglende deps.

**Live-scrape-knappen returnerer "FIRECRAWL_API_KEY mangler"**
→ Lokalt: tjek at `.env` har nøglen og at du startede streamlit fra mappen.
→ Cloud: tjek at Secrets indeholder nøglen i TOML-format.

**`dashboard.json` indlæses ikke**
→ Filen mangler. Kør først en scrape (lokalt eller via knap). Dataen committes ikke automatisk i v2 — Streamlit-appen skriver til lokal disk.

**Streamlit Cloud filsystem-persistence**
→ Streamlit Cloud's filsystem er ikke 100% persistent på tværs af restarts. `data/snapshots.db` kan blive nulstillet. `data/dashboard.json` overlever fordi den er committed til repoet.
→ Løsning hvis dette bliver et problem: Migration til Postgres / Supabase i en senere session.
