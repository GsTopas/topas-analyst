# scripts/archived/

Engangs-scripts der sandsynligvis er kørt og ikke længere er en del af det aktive code-flow.
Bevaret som reference. Importeres ikke fra produktion (`streamlit_app.py`, `pages/`, `topas_scraper/runner.py`).

**Backfill / migration (engangs):**
- `migrate_to_supabase.py` — SQLite → Supabase migration (kørt 2026)
- `backfill_duration_penalty.py` — duration-penalty backfill (n8n v22.2)
- `_backfill_approved.py` — review_decisions → approved_competitor_targets
- `_sync_ai_categories.py` — tour_category sync
- `_cleanup_legacy.py` — NPAP testdata-oprydning

**Debug / research scripts:**
- `debug_ruby_vision.py` — Ruby Korsika vision-debug
- `debug_stjernegaard_meals.py` — Stjernegaard meals-regex
- `debug_topas_meals.py` — Topas M/F/A-regex
- `debug_vnsn_scrape.py` — VNSN scrape-debug
- `_research_meals.py` — produceret outputs/meals_research/
- `_test_meals_extraction.py` — companion til `_research_meals.py`
- `_debug_duration.py` — duration-extraction debug (kør som `python -m ...`)

**Erstattet af in-process Python-pipeline:**
- `n8n_client.py` — pull-webhook-klient til n8n's "Competitor Analysis"-data table.
  Erstattet af `topas_scraper/competitor_search.py` der skriver direkte til Supabase
  (n8n-engine bevidst dropped 2026-05-15 efter side-om-side test viste Python = n8n).

Hvis et af disse skal gen-aktiveres: flyt tilbage til repo-rod eller `topas_scraper/`.
Filerne her bliver ikke kørt automatisk.
