#!/usr/bin/env python3
"""
Migrate old-schema rows (flattened: REEL 1/2 + PIC per day per account)
into the new per-post-per-snapshot format.

For each old row:
  - Extract shortcode from REEL 1, REEL 2, PIC URLs
  - Create 1-3 new-schema rows (one per post that has metrics)
  - Mark each with SCRAPE AGE = 'HISTORICAL' so you can filter them separately
  - Archive the original row (add ARCHIVED = TRUE)

Run:
  python migrate_old_schema.py --dry-run   # preview
  python migrate_old_schema.py            # execute
"""

import os, re, sys, time, argparse
from datetime import date, datetime, timezone
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN     = os.environ.get("AIRTABLE_PAT", "")
BASE_ID   = "appi9PUu4ZqKiOXkw"
TABLE_ID  = "tblCWODP44zR22p8D"

FIELDS_TO_KEEP_OLD = {
    # Fields that exist in old rows and should remain in the old row
    # (most old fields will be superseded by new-schema rows)
}
FIELDS_TO_DELETE_FROM_OLD = {
    # Fields from old schema that are redundant/bad data
}
POST_TYPE_MAP = {
    "REEL 1": "REEL",
    "REEL 2": "REEL",
    "PIC": "PIC",
}
METRIC_FIELDS = {
    "REEL 1": {
        "likes":   "LIKES REEL 1",
        "views":   "VIEWS REEL 1",
        "comments":"COMMENTS REEL 1",
        "reposts": "REPOSTS REEL 1",
        "sends":   "SENDS REEL 1",
    },
    "REEL 2": {
        "likes":   "LIKES REEL 2",
        "views":   "VIEWS REEL 2",
        "comments":"COMMENTS REEL 2",
        "reposts": "REPOSTS REEL 2",
        "sends":   "SENDS REEL 2",
    },
    "PIC": {
        "likes":   "LIKES POST 1",
        "comments":"COMMENTS POST 1",
    },
}

def shortcode_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"/(?:p|reel|tv)/([A-Za-z0-9_-]+)", url)
    return m.group(1) if m else ""

def eng_rate(likes, comments, followers):
    if not followers:
        return 0.0
    return round((likes + comments) / followers * 100, 2)

def at_headers():
    return {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def get_all_records():
    """Fetch ALL records, handling pagination."""
    all_records = []
    offset = ""
    while True:
        params = {"maxRecords": 100, "pageSize": 100}
        if offset:
            params["offset"] = offset
        r = requests.get(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}",
            headers=at_headers(),
            params=params,
            timeout=30,
        )
        r.raise_for_status()
        d = r.json()
        all_records.extend(d.get("records", []))
        offset = d.get("offset", "")
        if not offset:
            break
    return all_records

def build_new_rows_from_old(old_rec: dict) -> list[dict]:
    """
    Convert one old-schema row into 1-3 new-schema rows (one per post with metrics).
    Returns list of field dicts ready to POST.
    """
    f = old_rec["fields"]
    account     = f.get("ACCOUNT", "")
    profile_url = f.get("IG PROFILE URL", "")
    row_date    = f.get("DATE POSTED", "")
    weekday     = f.get("WEEKDAY", "")
    week_no     = f.get("WEEK NO", "")
    data_source = f.get("DATA SOURCE", "")
    followers   = f.get("FOLLOWERS AT SCRAPE", 0) or 0

    new_rows = []

    for post_field, post_type in [("REEL 1", "REEL"), ("REEL 2", "REEL"), ("PIC", "PIC")]:
        url = f.get(post_field, "")
        if not url:
            continue

        shortcode = shortcode_from_url(url)
        if not shortcode:
            continue

        metrics = METRIC_FIELDS.get(post_field, {})
        likes     = f.get(metrics.get("likes",    ""), 0) or 0
        views     = f.get(metrics.get("views",    ""), 0) or 0
        comments  = f.get(metrics.get("comments", ""), 0) or 0
        reposts   = f.get(metrics.get("reposts",  ""), 0) or 0
        sends     = f.get(metrics.get("sends",    ""), 0) or 0

        # Skip if no meaningful metrics at all
        if likes == 0 and views == 0 and comments == 0:
            continue

        fields = {
            "SHORTCODE":           shortcode,
            "ACCOUNT":             account,
            "IG PROFILE URL":      profile_url,
            "POST URL":            url,
            "POST TYPE":           post_type,
            "DATE POSTED":         row_date,
            "WEEKDAY":             weekday,
            "WEEK NO":             week_no,
            "SCRAPE DATE":         date.today().isoformat(),
            "SCRAPE AGE":          "HISTORICAL",
            "FOLLOWERS AT SCRAPE": followers,
            "LIKES":               likes,
            "COMMENTS":            comments,
            "VIEWS":               views,
            "PLAYS":               views,   # VIEWS == PLAYS in old data
            "SCRAPE STATUS":       "OK",
            "DATA SOURCE":         data_source or "MIGRATED",
            # Legacy fields for reference
            "REPOSTS REEL 1" if post_field == "REEL 1" else "REPOSTS REEL 2" if post_field == "REEL 2" else "REPOSTS PIC": reposts,
            "SENDS REEL 1" if post_field == "REEL 1" else "SENDS REEL 2" if post_field == "REEL 2" else "SENDS PIC": sends,
        }

        new_rows.append(fields)

    return new_rows

def run(dry_run: bool):
    print(f"\n{'='*60}")
    print(f"MIGRATION {'DRY RUN' if dry_run else 'EXECUTE'}")
    print(f"{'='*60}\n")

    print("Fetching all records...")
    all_records = get_all_records()
    print(f"Total records: {len(all_records)}")

    old_rows = [r for r in all_records if not r["fields"].get("SHORTCODE") and r["fields"].get("DATE POSTED")]
    new_rows_count = sum(1 for r in all_records if r["fields"].get("SHORTCODE"))
    print(f"Old-schema rows: {len(old_rows)}")
    print(f"New-schema rows: {new_rows_count}")
    print()

    total_new = 0
    total_archive = 0
    accounts_affected = set()

    for rec in old_rows:
        f = rec["fields"]
        account = f.get("ACCOUNT", "?")
        row_date = f.get("DATE POSTED", "?")
        new_row_data = build_new_rows_from_old(rec)

        if not new_row_data:
            print(f"  {account} | {row_date} — no new rows (no metrics)")
            continue

        accounts_affected.add(account)
        print(f"\n[{account}] {row_date} → {len(new_row_data)} new rows")

        for fields in new_row_data:
            total_new += 1
            likes_v = fields.get("LIKES", 0)
            views_v = fields.get("VIEWS", 0)
            sc = fields.get("SHORTCODE", "")
            print(f"    {sc[:12]} | {fields.get('POST TYPE','')} | likes={likes_v} views={views_v}")

            if not dry_run:
                r = requests.post(
                    f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}",
                    headers=at_headers(),
                    json={"fields": fields},
                    timeout=30,
                )
                if r.status_code not in (200, 201):
                    print(f"    ERROR {r.status_code}: {r.text[:100]}")
                else:
                    new_id = r.json().get("id", "?")
                    print(f"    ✓ Created {new_id}")
                time.sleep(0.3)

        # Archive the old row
        total_archive += 1
        if not dry_run:
            r = requests.patch(
                f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}/{rec['id']}",
                headers=at_headers(),
                json={"fields": {"ARCHIVED": True}},
                timeout=30,
            )
            if r.status_code == 200:
                print(f"    ✓ Archived old row {rec['id']}")
            else:
                print(f"    ERROR archiving: {r.status_code}")
            time.sleep(0.3)

    print(f"\n{'='*60}")
    if dry_run:
        print(f"✅ Dry run: would create {total_new} new rows, archive {total_archive} old rows")
    else:
        print(f"✅ Done: created {total_new} new rows, archived {total_archive} old rows")
        print(f"Accounts affected: {sorted(accounts_affected)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not TOKEN:
        sys.exit("ERROR: AIRTABLE_PAT not set in .env")
    run(args.dry_run)
