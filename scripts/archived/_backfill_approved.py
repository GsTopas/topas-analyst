"""
One-shot backfill: read existing review_decisions where the latest decision
per n8n candidate is 'approve', and upsert those into
approved_competitor_targets.

Run from topas-scraper folder:
    python -m topas_scraper._backfill_approved
"""
from topas_scraper import catalog_db


def main() -> None:
    conn = catalog_db.connect()

    rows = conn.execute(
        """
        WITH latest_decisions AS (
            SELECT target_id, action, reviewer, id AS decision_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY target_id ORDER BY decided_at DESC
                   ) AS rn
            FROM review_decisions
            WHERE target_kind = 'n8n_candidate'
        )
        SELECT
            c.competitor_domain, c.tour_url, c.topas_tour_code,
            c.tour_name, c.duration_days,
            l.decision_id, l.reviewer
        FROM n8n_candidates c
        INNER JOIN latest_decisions l
            ON l.target_id = c.n8n_row_id AND l.rn = 1
        WHERE l.action = 'approve'
          AND c.tour_url IS NOT NULL AND c.tour_url != ''
          AND c.topas_tour_code IS NOT NULL AND c.topas_tour_code != ''
        """
    ).fetchall()

    print(f"Found {len(rows)} approved candidates to backfill")
    added = 0
    for r in rows:
        d = dict(r)
        domain = d["competitor_domain"]
        name = d.get("tour_name") or d["tour_url"]
        was_added = catalog_db.upsert_approved_target(
            conn,
            operator=domain,
            tour_url=d["tour_url"],
            topas_tour_code=d["topas_tour_code"],
            tour_name=d.get("tour_name"),
            duration_days=d.get("duration_days"),
            approved_by=d.get("reviewer"),
            decision_id=d.get("decision_id"),
        )
        if was_added:
            added += 1
            print(f"  + {domain} | {name}")
        else:
            print(f"  . {domain} | {name} (already in approved table)")

    print(
        f"Backfilled {added} new approved targets "
        f"({len(rows) - added} already present)"
    )
    conn.close()


if __name__ == "__main__":
    main()
