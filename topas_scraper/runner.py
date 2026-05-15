"""
Reusable scrape orchestration.

Extracted from cli.py so that both the CLI and the Streamlit app can run a
scrape without going through subprocess. Streamlit's live-scrape button calls
`run_scrape_for_tour()` directly, gets per-target progress callbacks, and
re-renders the page with fresh data.

v0.8 architecture: schema-driven LLM extraction
  - Primary: Firecrawl JSON format (renders JS + LLM extracts via schema)
  - Tier 3 fallback: Firecrawl screenshot + Claude vision (same schema)
  - For operators with sitemap-listed URL variants (Albatros), augment with
    sitemap_discovery to ensure ALL departures are captured (lesson from the
    n8n-agent experiment: agents that fetch sitemap.xml find variants that
    single-page scraping misses).
  - Same downstream code for every operator.

v0.9: parallel scraping via ThreadPoolExecutor. Each target's Tier 1 Firecrawl
call runs concurrently (default 5 workers, configurable via SCRAPE_MAX_WORKERS
env-var). DB-writes and progress emits are serialized via locks so the data
layer stays simple — psycopg2 connections aren't thread-safe.
"""
from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Optional

from .client import FirecrawlClient
from .config import TARGETS, DEFAULT_DB_PATH, TourTarget, load_active_targets
from .db import (
    connect, start_run, finish_run, upsert_tour, replace_departures,
    fetch_departures,
)
from .export import export
from .extraction_schema import TOUR_EXTRACTION_SCHEMA
from .parsers import PARSERS
from .sitemap_discovery import (
    OPERATOR_VARIANT_PATTERNS,
    discover_variants,
    merge_variants_into_departures,
)


# Type for progress callbacks. The Streamlit app uses this to update a
# st.status() panel as each target is scraped. The CLI passes a printer.
ProgressCallback = Callable[[str], None]


# Default parallelism. Set via SCRAPE_MAX_WORKERS env-var.
# Hobby plan Firecrawl: 5 concurrent. Standard plan: 50 concurrent.
DEFAULT_MAX_WORKERS = 5


def run_scrape(
    targets: list[TourTarget],
    db_path: str | None = None,
    on_progress: Optional[ProgressCallback] = None,
) -> tuple[str, int, int]:
    """
    Run scrape against the given targets in parallel.

    Returns (run_id, success_count, total_count).

    The on_progress callback is invoked with status strings as each target
    is processed. UI layers (CLI / Streamlit) can render however they like.

    Parallelism: targets are processed concurrently with up to
    SCRAPE_MAX_WORKERS workers (default 5). Each worker handles one target's
    full Tier 1 → sitemap → Tier 3 → meals → DB-write cycle. DB-writes are
    serialized via lock since psycopg2 connections aren't thread-safe.
    """
    emit_lock = threading.Lock()
    db_lock = threading.Lock()

    def emit(msg: str) -> None:
        if on_progress is not None:
            with emit_lock:
                on_progress(msg)

    emit("Forbinder til Firecrawl...")
    client = FirecrawlClient()  # raises RuntimeError if API key missing

    # Lazy-init the vision extractor: only construct if any target needs it.
    # Avoids requiring ANTHROPIC_API_KEY for users who only scrape Tier 1 ops.
    vision = None
    needs_vision = any(t.vision_fallback for t in targets)
    if needs_vision:
        try:
            from .vision_extractor import VisionExtractor
            vision = VisionExtractor(client)
            emit("Tier 3 (Claude vision) fallback aktiveret")
        except RuntimeError as e:
            emit(f"⚠ Tier 3 ikke tilgængelig: {e}")
            emit("  Kør videre med Tier 1 only — operatører med JS-render vil mangle afgange")

    max_workers = int(os.getenv("SCRAPE_MAX_WORKERS", str(DEFAULT_MAX_WORKERS)))
    max_workers = max(1, min(max_workers, len(targets)))

    db = db_path or str(DEFAULT_DB_PATH)
    conn = connect(db)
    try:
        run_id = start_run(conn, target_count=len(targets))
        emit(f"Run {run_id[:8]} startet — scraper {len(targets)} URLs · {max_workers} workers")

        def process_one(i_target: tuple[int, TourTarget]) -> bool:
            """Process a single target end-to-end. Returns True on success.
            Designed to run concurrently; uses db_lock for DB-writes and
            emit_lock (via emit()) for progress output.
            """
            i, target = i_target
            total = len(targets)
            emit(f"[{i}/{total}] {target.operator}...")

            # Tier 1: Firecrawl with LLM-driven structured extraction via schema
            result = client.scrape(
                target.url,
                overrides=target.scrape_overrides,
                schema=TOUR_EXTRACTION_SCHEMA,
            )

            if not result.success:
                emit(f"[{i}/{total}] {target.operator} ✗ {result.error or 'no content'}")
                return False

            parser = PARSERS.get(target.parser_key)
            if not parser:
                emit(f"[{i}/{total}] {target.operator} ✗ no parser")
                return False

            try:
                tour_dict, departures = parser(result, target)
            except Exception as e:
                emit(f"[{i}/{total}] {target.operator} ✗ parse error: {e}")
                return False

            # ---- Sitemap-variant discovery (Albatros + similar) ----
            if target.operator in OPERATOR_VARIANT_PATTERNS:
                try:
                    variants = discover_variants(
                        target.operator,
                        target.url,
                        client.scrape,
                    )
                    if variants:
                        before = len(departures)
                        departures = merge_variants_into_departures(
                            departures,
                            variants,
                            from_price_dkk=tour_dict.get("from_price_dkk"),
                        )
                        added = len(departures) - before
                        if added > 0:
                            emit(
                                f"  ↳ Sitemap fandt {len(variants)} variant-URLs "
                                f"({added} nye afgange tilføjet)"
                            )
                            tour_dict["eligibility_notes"] = (
                                f"Tier 1 + sitemap-variants · {len(departures)} departures "
                                f"({added} from sitemap, rest from page)."
                            )
                except Exception as e:
                    emit(f"  ↳ Sitemap-discovery fejlede: {e}")

            # ---- Tier 3 fallback: invoke Claude vision ----
            tier_used = "T1"
            thin_result = (
                target.vision_fallback
                and vision is not None
                and len(departures) < 3
                and target.operator == "Albatros Travel"
            )
            if (not departures or thin_result) and target.vision_fallback and vision is not None:
                if departures:
                    emit(f"  ↳ Tier 1 fandt {len(departures)} afgange — kører Tier 3 (vision) for resten")
                else:
                    emit(f"  ↳ Tier 1 returnerede 0 afgange — falder tilbage til Tier 3 (vision)")
                try:
                    vision_deps = vision.extract(target.url, target.scrape_overrides)

                    vision_duration = getattr(vision, "last_tour_duration_days", None)
                    if vision_duration and not tour_dict.get("duration_days"):
                        tour_dict["duration_days"] = vision_duration
                        emit(f"  ↳ Tier 3: tour-duration sat til {vision_duration} dage")

                    if vision_deps:
                        existing_dates = {d["start_date"] for d in departures}
                        merged = list(departures)
                        added = 0
                        for vd in vision_deps:
                            if vd["start_date"] not in existing_dates:
                                merged.append(vd)
                                existing_dates.add(vd["start_date"])
                                added += 1
                        merged.sort(key=lambda d: d["start_date"])
                        if added > 0 or not departures:
                            departures = merged
                            tier_used = "T3" if not departures else "T1+T3"
                            if not departures:
                                tour_dict["eligibility_notes"] = (
                                    f"Extracted via Tier 3 vision (Firecrawl JSON returned 0). "
                                    f"{len(vision_deps)} departures recovered."
                                )
                            emit(f"  ↳ Tier 3: tilføjede {added} nye afgange (total: {len(merged)})")
                        else:
                            emit(f"  ↳ Tier 3: ingen nye afgange (T1 fangede alt)")
                    else:
                        emit(f"  ↳ Tier 3: ingen afgange fundet — siden er muligvis tom")
                except Exception as e:
                    emit(f"  ↳ Tier 3 fejlede: {e}")

            # Extract meals info from scraped markdown (best-effort).
            try:
                from .meals import extract_meals
                md = (result.markdown or "") if hasattr(result, "markdown") else ""
                if md and "stjernegaard-rejser.dk" in target.url:
                    dp_url = target.url.rstrip("/") + "/dagsprogram/"
                    try:
                        dp_result = client.scrape(dp_url)
                        if dp_result.success and getattr(dp_result, "markdown", None):
                            md = md + "\n\n" + dp_result.markdown
                            emit(f"  ↳ stjernegaard: dagsprogram hentet ({len(dp_result.markdown)} chars)")
                    except Exception as e:
                        emit(f"  ↳ stjernegaard: dagsprogram fetch fejlede: {e}")
                if md:
                    meals = extract_meals(md, url=target.url)
                    if meals.get("mealsCount") is not None:
                        tour_dict["meals_included"] = meals["mealsCount"]
                    if meals.get("mealsSummary"):
                        tour_dict["meals_description"] = meals["mealsSummary"]
                    method = meals.get("extractionMethod", "?")
                    emit(f"  ↳ meals: {meals.get('mealsCount')} ({method})")
            except Exception as exc:
                emit(f"  ↳ meals extraction skipped: {exc}")

            # DB-writes serialiseret via lock — psycopg2 connection er ikke
            # thread-safe. Tager kun ~50-200ms pr target, så serialisering
            # er en lille fraktion af total tid.
            with db_lock:
                upsert_tour(conn, tour_dict, run_id)
                existing_deps = fetch_departures(conn, target.operator, tour_dict["tour_slug"])
                existing_count = len(existing_deps)
                new_count = len(departures)
                degraded = False
                degraded_reason = ""

                if not departures and tour_dict.get("from_price_dkk"):
                    degraded = True
                    degraded_reason = "0 afgange men fra-pris fundet"
                elif existing_count >= 4 and new_count < int(existing_count * 0.75):
                    degraded = True
                    degraded_reason = f"thin result: {new_count} afgange (havde {existing_count})"

                if degraded:
                    emit(f"  ↳ ⚠ degraded scrape: {degraded_reason} — behold eksisterende DB-rækker")
                    dep_count = existing_count
                else:
                    dep_count = replace_departures(conn, target.operator, tour_dict["tour_slug"], departures, run_id)

            from_str = (
                f"fra {tour_dict.get('from_price_dkk')} kr."
                if tour_dict.get("from_price_dkk") else "no fra-pris"
            )
            tier_marker = f"[{tier_used}-DEGRADED]" if degraded else f"[{tier_used}]"
            emit(f"[{i}/{total}] {target.operator} ✓ {tier_marker} {dep_count} afgange · {from_str}")
            return True

        # Kør targets parallelt
        success = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_one, (i, t))
                for i, t in enumerate(targets, 1)
            ]
            for fut in as_completed(futures):
                try:
                    if fut.result():
                        success += 1
                except Exception as e:
                    emit(f"  ↳ uventet fejl i worker: {e}")

        finish_run(conn, run_id, success)
        emit(f"{success}/{len(targets)} succeeded")

        # Re-export JSON only if at least one target succeeded
        if success > 0:
            out = export(db)
            emit(f"Eksporteret dashboard JSON → {out}")

        return run_id, success, len(targets)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def run_scrape_for_tour(
    tour_code: str,
    db_path: str | None = None,
    on_progress: Optional[ProgressCallback] = None,
) -> tuple[str, int, int]:
    """
    Scrape only the targets for a specific Topas tour-code.

    For 'PTMD' this includes the Topas page + all 5 Madeira competitors.
    Cuts Firecrawl-credit usage to just what's needed for the chosen tour.
    """
    targets = load_active_targets(tour_code=tour_code)
    if not targets:
        raise ValueError(f"No targets configured for tour-code {tour_code!r}")
    return run_scrape(targets, db_path=db_path, on_progress=on_progress)



def run_scrape_all(
    db_path: str | None = None,
    on_progress: Optional[ProgressCallback] = None,
) -> tuple[str, int, int]:
    """Scrape ALL active targets across all configured tours."""
    targets = load_active_targets()
    if not targets:
        raise ValueError("No active targets configured")
    return run_scrape(targets, db_path=db_path, on_progress=on_progress)
