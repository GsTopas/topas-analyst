"""
One-shot sync: for each approved_competitor_targets row WITHOUT a tour_category,
look up the latest matching n8n_candidates row (by tour_url + topas_tour_code)
and copy its tour_category in.

Doesn't overwrite categories that are already set — preserves manual user edits.

Run from topas-scraper folder:
    python -m topas_scraper._sync_ai_categories
"""
from topas_scraper import catalog_db


def main() -> None:
    conn = catalog_db.connect()

    # Find approved targets missing a category
    missing = conn.execute(
        """
        SELECT id, operator, tour_url, topas_tour_code, tour_name
        FROM approved_competitor_targets
        WHERE tour_category IS NULL OR tour_category = ''
        """
    ).fetchall()

    print(f"Found {len(missing)} approved targets without category")
    if not missing:
        print("Nothing to sync.")
        return

    updated = 0
    skipped = 0
    for r in missing:
        d = dict(r)
        # Look up most recent n8n candidate for this URL + tour code
        cand = conn.execute(
            """
            SELECT tour_category
            FROM n8n_candidates
            WHERE tour_url = ? AND topas_tour_code = ?
              AND tour_category IS NOT NULL AND tour_category != ''
            ORDER BY n8n_row_id DESC
            LIMIT 1
            """,
            (d["tour_url"], d["topas_tour_code"]),
        ).fetchone()

        if cand and cand["tour_category"]:
            catalog_db.update_approved_target_category(
                conn,
                target_id=d["id"],
                tour_category=cand["tour_category"],
            )
            updated += 1
            print(
                f"  + {d['operator']} | {d.get('tour_name') or d['tour_url']} "
                f"-> {cand['tour_category']}"
            )
        else:
            skipped += 1
            print(
                f"  . {d['operator']} | {d.get('tour_name') or d['tour_url']} "
                f"(no AI category in n8n_candidates yet)"
            )

    print()
    print(f"Synced {updated} categories from AI · {skipped} still need manual category")
    conn.close()


if __name__ == "__main__":
    main()
