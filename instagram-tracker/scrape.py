#!/usr/bin/env python3
"""
ALJ Instagram Performance Tracker — Two-Phase System

Phase 1 — Discovery (runs twice daily, 6am & 6pm):
  Use instagram-post-scraper to find posts from the last 24h.
  Only CREATE new rows in Airtable. Never update existing records.

Phase 2 — Stats Capture (runs once daily, 6am):
  Find PENDING rows >= 5 days old.
  Use instagram-scraper to get fresh view/like/comment stats.
  Update those rows with stats. Never create new rows.

Usage:
  python scrape.py                   # run both phases
  python scrape.py --dry-run         # print what would happen, no writes
  python scrape.py --phase1-only     # discovery only (new posts)
  python scrape.py --phase2-only    # stats capture only (5-day+ posts)
"""

import os, sys, json, time, argparse
from datetime import date, datetime, timezone
import requests
from dotenv import load_dotenv

load_dotenv()

# -- CREDENTIALS ---------------------------------------------------------------

APIFY_TOKEN  = os.environ.get("APIFY_TOKEN", "")
AIRTABLE_PAT = os.environ.get("AIRTABLE_PAT", "")
BASE_ID      = "appi9PUu4ZqKiOXkw"
TABLE_ID     = "tbldSoSrRuAEdQ9Ya"

# -- ACCOUNTS ------------------------------------------------------------------

ACCOUNTS = [
    {"name": "RIN_JAPAN518",    "username": "RIN_JAPAN518"},
    {"name": "RINXRENX",        "username": "RINXRENX"},
    {"name": "REN.ABG",         "username": "REN.ABG"},
    {"name": "YOURELLAMIRA",    "username": "yourellamira"},
    {"name": "ELLA_ABG",        "username": "ella_abg"},
    {"name": "ELLAMOCHIMIRA_",  "username": "ellamochimira_"},
    {"name": "ONLYREXFIT",      "username": "onlyrexfit"},
    {"name": "_REXTYLER_",      "username": "_rextyler_"},
    {"name": "ONLYTYLERREX",    "username": "onlytylerrex"},
    {"name": "ABG.RICEBUNNY",   "username": "abg.ricebunny"},
]

# -- HELPERS -------------------------------------------------------------------

def post_type(t: str) -> str:
    if t in ("GraphVideo", "XDTGraphReel", "Video", "Reel"):
        return "REEL"
    if t in ("GraphSidecar", "Sidecar", "CAROUSEL"):
        return "CAROUSEL"
    return "PIC"

def week_number(d: date) -> str:
    return f"Week {d.isocalendar()[1]:02d}"

def shortcode_from_url(url: str) -> str:
    """Extract shortcode from an Instagram URL."""
    import re
    m = re.search(r"/(?:p|reel|tv)/([A-Za-z0-9_-]+)", url or "")
    return m.group(1) if m else ""

# -- APIFY ---------------------------------------------------------------------

def apify_post_scraper(usernames: list[str], newer_than: str = "1 day") -> list[dict]:
    """Use instagram-post-scraper to get posts from usernames. Returns list of post dicts."""
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(
        "https://api.apify.com/v2/acts/apify~instagram-post-scraper/runs",
        headers=headers,
        json={
            "dataDetailLevel": "basicData",
            "onlyPostsNewerThan": newer_than,
            "resultsLimit": 24,
            "skipPinnedPosts": False,
            "username": usernames,
        },
        timeout=30,
    )
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    print(f"  Apify run started: {run_id}")

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


def apify_scrape_post(post_url: str) -> dict:
    """Use instagram-scraper to get detailed stats for a single post URL."""
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(
        "https://api.apify.com/v2/acts/apify~instagram-scraper/runs",
        headers=headers,
        json={
            "directUrls": [post_url],
            "resultsType": "posts",
            "resultsLimit": 1,
        },
        timeout=30,
    )
    r.raise_for_status()
    run_id = r.json()["data"]["id"]

    for i in range(30):
        time.sleep(10)
        sr = requests.get(
            f"https://api.apify.com/v2/acts/apify~instagram-scraper/runs/{run_id}",
            headers=headers, timeout=15
        )
        sr.raise_for_status()
        status = sr.json()["data"]["status"]
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify run ended: {status}")
    else:
        raise TimeoutError("Apify timed out after 5 minutes")

    ds = sr.json()["data"]["defaultDatasetId"]
    ir = requests.get(
        f"https://api.apify.com/v2/datasets/{ds}/items?limit=5",
        headers=headers, timeout=30
    )
    ir.raise_for_status()
    items = ir.json()
    posts = [x for x in items if x.get("shortCode") or x.get("code")]
    return posts[0] if posts else {}

# -- AIRTABLE ------------------------------------------------------------------

def at_headers() -> dict:
    return {"Authorization": f"Bearer {AIRTABLE_PAT}", "Content-Type": "application/json"}

def at_request(method: str, url: str, payload: dict = None) -> dict:
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

def at_fetch_all_ids() -> dict[str, str]:
    """Return {shortcode: record_id} for all records."""
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
            sc = rec["fields"].get("SHORTCODE", "")
            if sc:
                index[sc] = rec["id"]
        offset = data.get("offset")
        if not offset:
            break
    return index

def at_fetch_pending(age_days: int = 5) -> list[dict]:
    """Fetch PENDING rows >= age_days old. Returns [{record_id, shortcode, post_url, date_posted}]."""
    pending = []
    now_utc = datetime.now(timezone.utc)
    offset = None
    while True:
        params = {
            "pageSize": 100,
            "fields[]": ["SHORTCODE", "POST URL", "DATE POSTED", "SCRAPE STATUS"],
        }
        if offset:
            params["offset"] = offset
        r = requests.get(
            f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}",
            headers=at_headers(), params=params, timeout=30
        )
        r.raise_for_status()
        data = r.json()
        for rec in data.get("records", []):
            f = rec["fields"]
            # Filter in Python — Airtable filter formulas can fail on field names with spaces
            status = f.get("SCRAPE STATUS") or f.get("SCRAPE_STATUS") or ""
            if status != "PENDING":
                continue
            date_posted = f.get("DATE POSTED") or f.get("DATE_POSTED") or ""
            if not date_posted:
                continue
            try:
                posted_date = date.fromisoformat(date_posted)
            except ValueError:
                continue
            posted_dt = datetime(posted_date.year, posted_date.month, posted_date.day, tzinfo=timezone.utc)
            hours_old = (now_utc - posted_dt).total_seconds() / 3600
            if hours_old >= age_days * 24:
                pending.append({
                    "record_id":   rec["id"],
                    "shortcode":   f.get("SHORTCODE") or f.get("SHORTCODE", ""),
                    "post_url":    f.get("POST URL") or f.get("POST_URL", ""),
                    "date_posted": date_posted,
                })
        offset = data.get("offset")
        if not offset:
            break
    return pending

def at_create(fields: dict) -> str:
    resp = at_request("post", f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}", {"fields": fields})
    return resp.get("id", "")

def at_update(record_id: str, fields: dict) -> None:
    at_request("patch", f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}/{record_id}", {"fields": fields})

# -- PHASE 1: DISCOVERY --------------------------------------------------------

def run_phase1(usernames: list[str], index: dict, dry_run: bool) -> dict:
    """
    Use instagram-post-scraper to find posts from the last 24h.
    Only CREATE new rows. Never update existing records.
    """
    stats = {"created": 0, "skipped": 0, "errors": 0}
    username_to_name = {a["username"]: a["name"] for a in ACCOUNTS}

    print("\n[Phase 1] Scraping last 24h posts...")
    posts = apify_post_scraper(usernames, newer_than="1 day")

    if not posts:
        print("  No posts scraped.")
        return stats

    for post in posts:
        sc = post.get("shortCode", "")
        if not sc:
            continue

        if sc in index:
            stats["skipped"] += 1
            continue  # Don't update — skip

        ts = post.get("timestamp", "")
        try:
            posted_dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).date() if ts else date.today()
        except (ValueError, TypeError):
            posted_dt = date.today()

        username = post.get("ownerUsername", "")
        name = username_to_name.get(username, username.upper())
        views    = post.get("videoPlayCount") or post.get("videoViewCount") or 0
        likes    = post.get("likesCount") or 0
        comments = post.get("commentsCount") or 0

        fields = {
            "SHORTCODE":     sc,
            "ACCOUNT":       name,
            "POST URL":      f"https://www.instagram.com/p/{sc}/",
            "POST TYPE":     post_type(post.get("type", "")),
            "DATE POSTED":   posted_dt.isoformat(),
            "WEEKDAY":       posted_dt.strftime("%A").upper(),
            "WEEK NO":       week_number(posted_dt),
            "LIKES":         likes,
            "COMMENTS":      comments,
            "VIEWS":         views,
            "PLAYS":         views,
            "SCRAPE STATUS": "PENDING",
            "SCRAPE DATE":   date.today().isoformat(),
        }

        if dry_run:
            print(f"  [DRY] CREATE {sc[:12]} ({name})")
            stats["created"] += 1
        else:
            try:
                at_create(fields)
                print(f"  + {name} | {sc[:12]} | NEW")
                stats["created"] += 1
            except Exception as e:
                print(f"  ERROR create {sc[:12]}: {e}")
                stats["errors"] += 1

        time.sleep(0.25)

    return stats

# -- PHASE 2: STATS CAPTURE ----------------------------------------------------

def run_phase2(dry_run: bool) -> dict:
    """
    Find PENDING rows >= 5 days old.
    Use instagram-scraper to get fresh stats.
    Only UPDATE — never create.
    """
    stats = {"captured": 0, "still_pending": 0, "errors": 0}
    pending_rows = at_fetch_pending(age_days=5)

    if not pending_rows:
        print("\n[Phase 2] No PENDING rows >= 5 days old.")
        return stats

    print(f"\n[Phase 2] Found {len(pending_rows)} PENDING rows >= 5 days old")
    print("  Scraping stats from Apify...")

    for row in pending_rows:
        sc       = row["shortcode"]
        post_url = row["post_url"]
        record_id = row["record_id"]

        if not post_url:
            stats["errors"] += 1
            continue

        print(f"  Scraping {sc[:12]}...", end=" ", flush=True)

        if dry_run:
            print(f"[DRY] WOULD SCRAPE")
            stats["captured"] += 1
            continue

        try:
            post = apify_scrape_post(post_url)
        except Exception as e:
            print(f"ERROR: {e}")
            stats["errors"] += 1
            time.sleep(1)
            continue

        views    = post.get("videoPlayCount") or post.get("videoViewCount") or 0
        likes    = post.get("likesCount") or 0
        comments = post.get("commentsCount") or 0
        today    = date.today().isoformat()

        fields = {
            "LIKES":          likes,
            "COMMENTS":       comments,
            "VIEWS":          views,
            "PLAYS":          views,
            "SCRAPE STATUS":  "CAPTURED",
            "SCRAPE DATE":    today,
        }

        try:
            at_update(record_id, fields)
            print(f"captured (likes:{likes} views:{views})")
            stats["captured"] += 1
        except Exception as e:
            print(f"ERROR: {e}")
            stats["errors"] += 1

        time.sleep(1)  # Be nice to Apify

    return stats

# -- MAIN ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="ALJ Instagram Two-Phase Scraper")
    parser.add_argument("--dry-run",      action="store_true", help="Print what would happen, no writes")
    parser.add_argument("--phase1-only", action="store_true", help="Discovery only (new posts in last 24h)")
    parser.add_argument("--phase2-only",  action="store_true", help="Stats capture only (PENDING rows >= 5 days)")
    args = parser.parse_args()

    if not APIFY_TOKEN:
        sys.exit("ERROR: APIFY_TOKEN not set")
    if not AIRTABLE_PAT:
        sys.exit("ERROR: AIRTABLE_PAT not set")

    run_phase1_flag = not args.phase2_only
    run_phase2_flag = not args.phase1_only
    usernames = [a["username"] for a in ACCOUNTS]
    today = date.today()

    print(f"\n{'=' * 60}")
    print(f"ALJ INSTAGRAM SCRAPER  --  {today}")
    print(f"Phase 1 (Discovery): {'YES' if run_phase1_flag else 'NO'}")
    print(f"Phase 2 (Stats):     {'YES' if run_phase2_flag else 'NO'}")
    if args.dry_run:
        print("DRY RUN -- nothing will be written")
    print(f"{'=' * 60}\n")

    p1_stats = {"created": 0, "skipped": 0, "errors": 0}
    p2_stats = {"captured": 0, "still_pending": 0, "errors": 0}

    if run_phase1_flag:
        index = at_fetch_all_ids()
        print(f"  Existing records: {len(index)}")
        p1_stats = run_phase1(usernames, index, args.dry_run)

    if run_phase2_flag:
        p2_stats = run_phase2(args.dry_run)

    # Summary
    print(f"\n{'=' * 60}")
    print("=== RUN SUMMARY ===")
    if run_phase1_flag:
        print(f"Phase 1: {p1_stats['created']} new posts created, {p1_stats['skipped']} already existed (skipped)")
    if run_phase2_flag:
        print(f"Phase 2: {p2_stats['captured']} posts captured, {p2_stats['errors']} errors")
    if p1_stats.get("errors") or p2_stats.get("errors"):
        print(f"Errors: {p1_stats.get('errors', 0) + p2_stats.get('errors', 0)}")
    print("===================")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
