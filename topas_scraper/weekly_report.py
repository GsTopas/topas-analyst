"""
Ugentlig ændringsrapport — pris- og status-skift på enkelte afgange.

Brug:
    python -m topas_scraper.weekly_report                       # print til stdout
    python -m topas_scraper.weekly_report --out report.md       # gem til fil
    python -m topas_scraper.weekly_report --lookback 14         # 14-dages vindue
    python -m topas_scraper.weekly_report --top 20              # top 20 pr kategori
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from .db import connect


def fetch_recent_snapshots(conn, lookback_days=14):
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days * 2)
    cur = conn.execute(
        """
        SELECT operator, tour_slug, start_date,
               availability_status, price_dkk, observed_at
        FROM snapshots
        WHERE observed_at >= ?
        ORDER BY operator, tour_slug, start_date, observed_at DESC
        """,
        (cutoff.isoformat(),),
    )
    grouped = defaultdict(list)
    for row in cur.fetchall():
        key = (row["operator"], row["tour_slug"], str(row["start_date"]))
        grouped[key].append(dict(row))
    return grouped


def fetch_tour_index(conn):
    """Alle ture (Topas + konkurrenter) ligger i samme tours-tabel.
    Map (operator, tour_slug) → {tour_name, url, competes_with}."""
    out = {}
    cur = conn.execute(
        "SELECT operator, tour_slug, tour_name, url, competes_with FROM tours"
    )
    for row in cur.fetchall():
        out[(row["operator"], row["tour_slug"])] = {
            "tour_name": row["tour_name"],
            "url": row["url"],
            "competes_with": row["competes_with"],
        }
    return out


def compute_price_change(snapshots, lookback_days):
    if len(snapshots) < 2:
        return None
    valid = [s for s in snapshots if s.get("price_dkk") is not None]
    if len(valid) < 2:
        return None
    latest = valid[0]
    now_dt = _parse_dt(latest["observed_at"])
    threshold = now_dt - timedelta(days=lookback_days)
    baseline = None
    for s in valid[1:]:
        if _parse_dt(s["observed_at"]) <= threshold:
            baseline = s
            break
    if baseline is None:
        baseline = valid[-1]
    delta = latest["price_dkk"] - baseline["price_dkk"]
    if delta == 0:
        return None
    pct = (delta / baseline["price_dkk"]) * 100 if baseline["price_dkk"] else None
    return {
        "delta": delta,
        "pct": pct,
        "previous_price": baseline["price_dkk"],
        "current_price": latest["price_dkk"],
        "previous_observed_at": baseline["observed_at"],
        "current_observed_at": latest["observed_at"],
        "days_ago": (now_dt - _parse_dt(baseline["observed_at"])).days,
    }


def compute_status_change(snapshots):
    if len(snapshots) < 2:
        return None

    def cat(s):
        sl = (s or "").strip().lower()
        if sl in ("aaben", "ledig", "garanteret"):
            return "selling"
        if sl == "faa pladser":
            return "late_selling"
        if sl in ("paa foresporgsel", "afventer pris"):
            return "withdrawn"
        if sl == "udsolgt":
            return "sold_out"
        if "ben" in sl or "edig" in sl:
            return "selling"
        if "pladser" in sl:
            return "late_selling"
        if "foresp" in sl or "afvent" in sl:
            return "withdrawn"
        return "unknown"

    latest = snapshots[0]
    latest_cat = cat(latest["availability_status"])
    for prev in snapshots[1:]:
        prev_cat = cat(prev["availability_status"])
        if prev_cat != latest_cat:
            return {
                "from": prev["availability_status"],
                "from_category": prev_cat,
                "to": latest["availability_status"],
                "to_category": latest_cat,
                "previous_observed_at": prev["observed_at"],
                "current_observed_at": latest["observed_at"],
            }
    return None


def _parse_dt(s):
    if isinstance(s, datetime):
        return s if s.tzinfo else s.replace(tzinfo=timezone.utc)
    if isinstance(s, str):
        s = s.replace("Z", "+00:00")
        try:
            d = datetime.fromisoformat(s)
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except ValueError:
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)


def build_report(conn, lookback_days=7, top_n=10):
    grouped = fetch_recent_snapshots(conn, lookback_days=lookback_days)
    tour_index = fetch_tour_index(conn)

    price_changes = []
    new_sold_out = []
    new_guaranteed = []
    other_status = []

    for (operator, slug, start_date), snapshots in grouped.items():
        if len(snapshots) < 2:
            continue
        meta = tour_index.get((operator, slug), {})
        tour_name = meta.get("tour_name") or slug
        competes_with = meta.get("competes_with") or "-"

        change = compute_price_change(snapshots, lookback_days=lookback_days)
        if change:
            price_changes.append({
                "operator": operator, "tour_name": tour_name,
                "start_date": start_date, "competes_with": competes_with,
                "url": meta.get("url"), **change,
            })

        status_change = compute_status_change(snapshots)
        if status_change:
            entry = {
                "operator": operator, "tour_name": tour_name,
                "start_date": start_date, "competes_with": competes_with,
                "url": meta.get("url"), **status_change,
            }
            if status_change["to_category"] == "sold_out":
                new_sold_out.append(entry)
            elif "garant" in (status_change["to"] or "").lower():
                new_guaranteed.append(entry)
            else:
                other_status.append(entry)

    rises = sorted([c for c in price_changes if c["delta"] > 0], key=lambda c: -c["delta"])[:top_n]
    falls = sorted([c for c in price_changes if c["delta"] < 0], key=lambda c: c["delta"])[:top_n]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out = []
    out.append("# Ugentlig aendringsrapport - " + now)
    out.append("")
    out.append("Sammenligning over de seneste **" + str(lookback_days) + " dage**. "
               "Kilde: snapshots-tabellen (Supabase).")
    out.append("")
    out.append("## Oversigt")
    out.append("")
    out.append("- Pris-aendringer: **" + str(len(price_changes)) + "** afgange "
               "(" + str(len(rises)) + " stigninger, " + str(len(falls)) + " fald)")
    out.append("- Nye Udsolgt: **" + str(len(new_sold_out)) + "** afgange")
    out.append("- Nye Garanteret: **" + str(len(new_guaranteed)) + "** afgange")
    out.append("- Ovrige status-skift: **" + str(len(other_status)) + "** afgange")
    out.append("")

    out.append("## Top " + str(len(rises)) + " prisstigninger")
    out.append("")
    if rises:
        out.append("| Operator | Tur | Afgang | Tur-kode | For | Nu | Delta kr | Delta % |")
        out.append("|---|---|---|---|---:|---:|---:|---:|")
        for c in rises:
            pct = ("%+.1f%%" % c["pct"]) if c.get("pct") is not None else "-"
            tm = "[" + c["tour_name"] + "](" + (c.get("url") or "") + ")" if c.get("url") else c["tour_name"]
            out.append("| " + c["operator"] + " | " + tm + " | " + str(c["start_date"]) +
                       " | " + c["competes_with"] + " | " + _dkk(c["previous_price"]) +
                       " | " + _dkk(c["current_price"]) + " | +" + _int(c["delta"]) + " | " + pct + " |")
    else:
        out.append("_Ingen prisstigninger observeret._")
    out.append("")

    out.append("## Top " + str(len(falls)) + " prisfald")
    out.append("")
    if falls:
        out.append("| Operator | Tur | Afgang | Tur-kode | For | Nu | Delta kr | Delta % |")
        out.append("|---|---|---|---|---:|---:|---:|---:|")
        for c in falls:
            pct = ("%+.1f%%" % c["pct"]) if c.get("pct") is not None else "-"
            tm = "[" + c["tour_name"] + "](" + (c.get("url") or "") + ")" if c.get("url") else c["tour_name"]
            out.append("| " + c["operator"] + " | " + tm + " | " + str(c["start_date"]) +
                       " | " + c["competes_with"] + " | " + _dkk(c["previous_price"]) +
                       " | " + _dkk(c["current_price"]) + " | " + _int(c["delta"]) + " | " + pct + " |")
    else:
        out.append("_Ingen prisfald observeret._")
    out.append("")

    out.append("## Nye Udsolgt-afgange (" + str(len(new_sold_out)) + ")")
    out.append("")
    if new_sold_out:
        out.append("| Operator | Tur | Afgang | Tur-kode | Var | Nu |")
        out.append("|---|---|---|---|---|---|")
        for s in new_sold_out:
            tm = "[" + s["tour_name"] + "](" + (s.get("url") or "") + ")" if s.get("url") else s["tour_name"]
            out.append("| " + s["operator"] + " | " + tm + " | " + str(s["start_date"]) +
                       " | " + s["competes_with"] + " | " + str(s["from"]) +
                       " | **" + str(s["to"]) + "** |")
    else:
        out.append("_Ingen nye Udsolgt-afgange._")
    out.append("")

    out.append("## Nye Garanteret-afgange (" + str(len(new_guaranteed)) + ")")
    out.append("")
    if new_guaranteed:
        out.append("| Operator | Tur | Afgang | Tur-kode | Var | Nu |")
        out.append("|---|---|---|---|---|---|")
        for s in new_guaranteed:
            tm = "[" + s["tour_name"] + "](" + (s.get("url") or "") + ")" if s.get("url") else s["tour_name"]
            out.append("| " + s["operator"] + " | " + tm + " | " + str(s["start_date"]) +
                       " | " + s["competes_with"] + " | " + str(s["from"]) +
                       " | **" + str(s["to"]) + "** |")
    else:
        out.append("_Ingen nye Garanteret-afgange._")
    out.append("")

    if other_status:
        out.append("## Ovrige status-skift (" + str(len(other_status)) + ")")
        out.append("")
        out.append("| Operator | Tur | Afgang | Tur-kode | Var | Nu |")
        out.append("|---|---|---|---|---|---|")
        for s in other_status[:top_n]:
            tm = "[" + s["tour_name"] + "](" + (s.get("url") or "") + ")" if s.get("url") else s["tour_name"]
            out.append("| " + s["operator"] + " | " + tm + " | " + str(s["start_date"]) +
                       " | " + s["competes_with"] + " | " + str(s["from"]) +
                       " | " + str(s["to"]) + " |")
        out.append("")

    out.append("---")
    out.append("_Genereret " + datetime.now(timezone.utc).isoformat(timespec="seconds") + "_")
    return "\n".join(out)


def _dkk(v):
    if v is None:
        return "-"
    try:
        return ("{:,}".format(int(v)).replace(",", ".") + " kr")
    except (TypeError, ValueError):
        return "-"


def _int(v):
    if v is None:
        return "-"
    try:
        return "{:,}".format(abs(int(v))).replace(",", ".")
    except (TypeError, ValueError):
        return "-"


def main(argv=None):
    p = argparse.ArgumentParser(description="Generer ugentlig aendringsrapport.")
    p.add_argument("--out", help="Gem rapport til fil (markdown).")
    p.add_argument("--lookback", type=int, default=7)
    p.add_argument("--top", type=int, default=10)
    args = p.parse_args(argv)

    conn = connect()
    report = build_report(conn, lookback_days=args.lookback, top_n=args.top)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(report)
        print("Rapport gemt -> " + args.out, file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
