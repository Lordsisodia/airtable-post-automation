#!/usr/bin/env python3
"""
ALJ Instagram Performance Tracker — Single-Phase Upsert

Scrapes posts from all accounts (last 3 days), then upserts to Airtable:
  - Existing rows: update with fresh DAY3 stats
  - New rows: create with PENDING status

Actor: apify~instagram-post-scraper (basicData, all accounts in one call)
"""

import os, json, time, argparse
from datetime import date
import requests
from dotenv import load_dotenv

load_dotenv()

# -- CREDENTIALS ---------------------------------------------------------------

APIFY_TOKEN  = os.environ.get("APIFY_TOKEN", "")
AIRTABLE_PAT = os.environ.get("AIRTABLE_PAT", "")
BASE_ID      = "appi9PUu4ZqKiOXkw"
TABLE_ID     = "tbldSoSrRuAEdQ9Ya"

# -- ACCOUNTS (username for API, display name for Airtable) ---------------------

ACCOUNTS = [
    {"name": "RIN_JAPAN518",   "username": "RIN_JAPAN518"},
    {"name": "RINXRENX",       "username": "RINXRENX"},
    {"name": "REN.ABG",        "username": "REN.ABG"},
    {"name": "YOURELLAMIRA",   "username": "yourellamira"},
    {"name": "ELLA_ABG",       "username": "ella_abg"},
    {"name": "ELLAMOCHIMIRA_", "username": "ellamochimira_"},
    {"name": "ONLYREXFIT",     "username": "onlyrexfit"},
    {"name": "_REXTYLER_",     "username": "_rextyler_"},
    {"name": "ONLYTYLERREX",   "username": "onlytylerrex"},
    {"name": "ABG.RICEBUNNY",  "username": "abg.ricebunny"},
]

# -- HELPERS -------------------------------------------------------------------

def post_type(t: str) -> str:
    if t in ("GraphVideo", "XDTGraphReel", "Video", "Reel"):
        return "REEL"
    if t in ("GraphSidecar", "Sidecar", "CAROUSEL"):
        return "CAROUSEL"
    return "PIC"

def week_number(d) -> str:
    return f"Week {d.isocalendar()[1]:02d}"

# -- APIFY ---------------------------------------------------------------------

def apify_scrape(usernames: list[str]) -> list[dict]:
    """Scrape posts from all usernames in one call. Returns list of post dicts."""
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(
        "https://api.apify.com/v2/acts/apify~instagram-post-scraper/runs",
        headers=headers,
        json={
            "dataDetailLevel": "basicData",
            "onlyPostsNewerThan": "3 days",
            "resultsLimit": 24,
            "skipPinnedPosts": False,
            "username": usernames,
        },
        timeout=30,
    )
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    print(f"  Apify run: {run_id}")

    for i in range(60):
        time.sleep(10)
        sr = requests.get(
            f"https://api.apify.com/v2/acts/apify~instagram-post-scraper/runs/{run_id}",
            headers=headers, timeout=15
        )
        sr.raise_for_status()
        status = sr.json()["data"]["status"]
        if i % 3 == 0:
            print(f"  [{i * 10}s] {status}")
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify run ended: {status}")
    else:
        raise TimeoutError("Apify timed out after 10 minutes")

    ds = sr.json()["data"]["defaultDatasetId"]
    ir = requests.get(
        f"https://api.apify.com/v2/datasets/{ds}/items?limit=500",
        headers=headers, timeout=30
    )
    ir.raise_for_status()
    items = ir.json()
    posts = [x for x in items if x.get("shortCode")]
    print(f"  Scraped {len(posts)} posts from {len(usernames)} accounts")
    return posts

# -- AIRTABLE ------------------------------------------------------------------

def at_headers() -> dict:
    return {"Authorization": f"Bearer {AIRTABLE_PAT}", "Content-Type": "application/json"}

def at_write(method: str, url: str, payload: dict) -> dict:
    for attempt in range(3):
        try:
            r = requests.request(method, url, headers=at_headers(), json=payload, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ReadTimeout:
            if attempt == 2:
                raise
            wait = 2 ** attempt
            print(f"    Retry {attempt + 1} in {wait}s...")
            time.sleep(wait)
    return {}

def at_fetch_index() -> dict[str, str]:
    """Return {shortcode: record_id} for all existing records."""
    index = {}
    offset = None
    while True:
        params = {"pageSize": 100, "fields[]": ["SHORTCODE"]}
        if offset:
            params["offset"] = offset
        r = requests.get(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}",
            headers=at_headers(), params=params, timeout=30
        )
        r.raise_for_status()
        data = r.json()
        for rec in data.get("records", []):
            sc = rec.get("fields", {}).get("SHORTCODE", "")
            if sc:
                index[sc] = rec["id"]
        offset = data.get("offset")
        if not offset:
            break
    return index

def at_create(fields: dict) -> str:
    resp = at_write("post", f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}", {"fields": fields})
    return resp.get("id", "")

def at_update(record_id: str, fields: dict) -> None:
    at_write("patch", f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}/{record_id}", {"fields": fields})

# -- UPSERT --------------------------------------------------------------------

def upsert_posts(posts: list[dict], index: dict, dry_run: bool) -> dict:
    """
    For each scraped post:
      - If shortcode exists in Airtable → update DAY3 stats
      - If new → create PENDING row
    """
    stats = {"updated": 0, "created": 0, "errors": 0}

    # Build username → display name lookup
    username_to_name = {a["username"]: a["name"] for a in ACCOUNTS}

    for post in posts:
        sc = post.get("shortCode", "")
        if not sc:
            continue

        ts = post.get("timestamp", "")
        try:
            from datetime import datetime, timezone
            posted_dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).date() if ts else date.today()
        except (ValueError, TypeError):
            posted_dt = date.today()

        username = post.get("ownerUsername", "")
        name = username_to_name.get(username, username.upper())

        views    = post.get("videoPlayCount") or post.get("videoViewCount") or 0
        likes    = post.get("likesCount") or 0
        comments = post.get("commentsCount") or 0
        today    = date.today().isoformat()

        if sc in index:
            # Update existing row
            fields = {
                "DAY3_VIEWS":     views,
                "DAY3_LIKES":     likes,
                "DAY3_COMMENTS":  comments,
                "DAY3_DATE":      today,
                "SCRAPE_STATUS":  "CAPTURED",
            }
            if dry_run:
                print(f"  [DRY] UPDATE {sc[:12]} → CAPTURED")
                stats["updated"] += 1
            else:
                try:
                    at_update(index[sc], fields)
                    print(f"  ~ {name} | {sc[:12]} | likes:{likes} | views:{views} | CAPTURED")
                    stats["updated"] += 1
                except Exception as e:
                    print(f"  ERROR update {sc[:12]}: {e}")
                    stats["errors"] += 1
        else:
            # Create new row
            fields = {
                "SHORTCODE":      sc,
                "ACCOUNT":        name,
                "POST_URL":       f"https://www.instagram.com/p/{sc}/",
                "POST_TYPE":      post_type(post.get("type", "")),
                "DATE_POSTED":    posted_dt.isoformat(),
                "WEEKDAY":        posted_dt.strftime("%A").upper(),
                "WEEK_NO":        week_number(posted_dt),
                "DAY3_LIKES":     likes,
                "DAY3_COMMENTS":  comments,
                "DAY3_VIEWS":     views,
                "SCRAPE_STATUS":  "CAPTURED",
            }
            if dry_run:
                print(f"  [DRY] CREATE {sc[:12]} ({name})")
                stats["created"] += 1
            else:
                try:
                    at_create(fields)
                    print(f"  + {name} | {sc[:12]} | likes:{likes} | views:{views} | NEW")
                    stats["created"] += 1
                except Exception as e:
                    print(f"  ERROR create {sc[:12]}: {e}")
                    stats["errors"] += 1

        time.sleep(0.25)

    return stats

# -- MAIN ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="ALJ Instagram scraper — single-phase upsert")
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen, no writes")
    args = parser.parse_args()

    if not APIFY_TOKEN:
        sys.exit("ERROR: APIFY_TOKEN not set")
    if not AIRTABLE_PAT:
        sys.exit("ERROR: AIRTABLE_PAT not set")

    usernames = [a["username"] for a in ACCOUNTS]
    today = date.today()
    print(f"\n{'=' * 60}")
    print(f"ALJ INSTAGRAM SCRAPER  --  {today}")
    print(f"Accounts: {len(usernames)}  |  Actor: instagram-post-scraper (basicData)")
    if args.dry_run:
        print("DRY RUN -- nothing will be written")
    print(f"{'=' * 60}\n")

    # Scrape all accounts in one call
    print("[1] Scraping posts from Apify...")
    posts = apify_scrape(usernames)

    if not posts:
        print("No posts scraped. Exiting.")
        return

    # Fetch existing Airtable index
    print("\n[2] Fetching Airtable index...")
    index = at_fetch_index()
    print(f"  {len(index)} existing records")

    # Upsert
    print(f"\n[3] Upserting to Airtable...")
    stats = upsert_posts(posts, index, args.dry_run)

    print(f"\n{'=' * 60}")
    print(f"=== DONE ===")
    print(f"  Updated: {stats['updated']}")
    print(f"  Created: {stats['created']}")
    print(f"  Errors:  {stats['errors']}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    import sys
    main()
