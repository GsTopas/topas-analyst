"""
Command-line interface.

Usage:
    python -m topas_scraper.cli scrape    # full pipeline: scrape, parse, store, export
    python -m topas_scraper.cli report    # print summary of latest run
    python -m topas_scraper.cli diff      # diff latest vs previous run
    python -m topas_scraper.cli export    # re-export JSON from latest run

The scrape command is the main workhorse. report and diff are inspection
tools that don't hit the network.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Windows consoles (cp1252) crash on Unicode arrows / checkmarks. Force UTF-8 stdout.
# Safe no-op on platforms that already use UTF-8.
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from dotenv import load_dotenv

from .client import FirecrawlClient
from .config import TARGETS, DEFAULT_DB_PATH
from .db import (
    connect, start_run, finish_run, upsert_tour, replace_departures,
    latest_run_id, previous_run_id, fetch_tours, fetch_departures, fetch_snapshot_for_run,
)
from .export import export
from .parsers import PARSERS
from .runner import run_scrape_all, run_scrape_for_tour


def cmd_scrape(args) -> int:
    """Full pipeline: scrape every target, parse, write to DB, export JSON."""
    load_dotenv()

    def emit(msg: str) -> None:
        # CLI just prints — Streamlit uses a different callback.
        print(f"→ {msg}")

    try:
        if args.tour:
            run_id, success, total = run_scrape_for_tour(args.tour, db_path=args.db, on_progress=emit)
        else:
            run_id, success, total = run_scrape_all(db_path=args.db, on_progress=emit)
    except RuntimeError as e:
        print(f"✗ {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"✗ {e}", file=sys.stderr)
        return 1

    return 0 if success == total else 1


def cmd_report(args) -> int:
    """Print summary of latest run."""
    conn = connect(args.db)
    run_id = latest_run_id(conn)
    if not run_id:
        print("No scraper runs in database.", file=sys.stderr)
        return 1

    rows = fetch_tours(conn, run_id)
    print(f"\nSnapshot — run {run_id[:8]}")
    print("─" * 80)
    for row in sorted(rows, key=lambda r: r["operator"]):
        deps = fetch_departures(conn, row["operator"], row["tour_slug"])
        priced = [d for d in deps if d["price_dkk"]]
        sellouts = [d for d in deps if d["availability_status"] == "Udsolgt"]
        from_str = f"{row['from_price_dkk']:>7,}".replace(",", ".") if row["from_price_dkk"] else "    —  "
        print(f"  {row['operator']:<18}  fra {from_str} kr.   "
              f"{len(deps):>2} afgange ({len(priced)} priced, {len(sellouts)} udsolgt)")
        if row["eligibility_notes"]:
            print(f"      ↳ {row['eligibility_notes']}")
    print()
    return 0


def cmd_diff(args) -> int:
    """Diff latest run against previous run."""
    conn = connect(args.db)
    latest = latest_run_id(conn)
    previous = previous_run_id(conn)
    if not latest:
        print("No scraper runs in database.", file=sys.stderr)
        return 1
    if not previous:
        print("Only one run in database — nothing to diff against.")
        return 0

    latest_snaps = {(s["operator"], s["tour_slug"], s["start_date"]): s for s in fetch_snapshot_for_run(conn, latest)}
    previous_snaps = {(s["operator"], s["tour_slug"], s["start_date"]): s for s in fetch_snapshot_for_run(conn, previous)}

    print(f"\nDiff: {previous[:8]} → {latest[:8]}")
    print("─" * 80)
    changes = 0
    for key, new_snap in latest_snaps.items():
        old_snap = previous_snaps.get(key)
        if not old_snap:
            print(f"  + NEW   {key[0]:<18} {key[2]}  {new_snap['price_dkk']} kr.  ({new_snap['availability_status']})")
            changes += 1
            continue
        price_changed = old_snap["price_dkk"] != new_snap["price_dkk"]
        status_changed = old_snap["availability_status"] != new_snap["availability_status"]
        if price_changed or status_changed:
            old_p = f"{old_snap['price_dkk']}" if old_snap["price_dkk"] else "—"
            new_p = f"{new_snap['price_dkk']}" if new_snap["price_dkk"] else "—"
            note = []
            if price_changed:
                note.append(f"pris {old_p} → {new_p}")
            if status_changed:
                note.append(f"status {old_snap['availability_status']} → {new_snap['availability_status']}")
            print(f"  ~ CHG   {key[0]:<18} {key[2]}  {' · '.join(note)}")
            changes += 1

    for key in previous_snaps.keys() - latest_snaps.keys():
        old = previous_snaps[key]
        print(f"  - GONE  {key[0]:<18} {key[2]}  was {old['price_dkk']} kr.")
        changes += 1

    if changes == 0:
        print("  (no changes)")
    print(f"\n{changes} change(s) detected.\n")
    return 0


def cmd_export(args) -> int:
    """Re-export JSON for the dashboard from the latest run."""
    out = export(args.db, output=args.output)
    print(f"→ Wrote {out}")
    return 0


def main(argv: list[str] = None) -> int:
    parser = argparse.ArgumentParser(prog="topas-scraper", description="Topas pricing scraper — Madeira slice.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="SQLite path (default: data/snapshots.db)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_scrape = sub.add_parser("scrape", help="Scrape all configured URLs and update DB.")
    p_scrape.add_argument(
        "--tour",
        type=str,
        default=None,
        help="Optional Topas tour-code (e.g. PTMD) — scrape only that tour and its competitors.",
    )
    sub.add_parser("report", help="Print summary of latest snapshot.")
    sub.add_parser("diff",   help="Diff latest run vs previous run.")
    p_export = sub.add_parser("export", help="Re-export dashboard JSON from latest run.")
    p_export.add_argument("--output", type=Path, help="Output path (default: data/dashboard.json)")

    args = parser.parse_args(argv)
    # Default --tour to None for non-scrape commands so cmd_scrape can read args.tour safely
    if not hasattr(args, "tour"):
        args.tour = None

    handlers = {
        "scrape": cmd_scrape,
        "report": cmd_report,
        "diff": cmd_diff,
        "export": cmd_export,
    }
    return handlers[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
