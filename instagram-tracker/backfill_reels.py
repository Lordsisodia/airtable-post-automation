#!/usr/bin/env python3
"""
ALJ Instagram Reel Backfill — One-time historical scrape using instagram-reel-scraper.

This script uses apify/instagram-reel-scraper to get ALL reels from each account
(historical, not just last 24h) and creates records in Airtable.

Run once to backfill, then rely on daily Phase 1 for new content.
"""
import os, sys, json, time, argparse
from datetime import date, datetime, timezone
import requests

APIFY_TOKEN  = os.environ.get("APIFY_TOKEN", "")
AIRTABLE_PAT = os.environ.get("AIRTABLE_PAT", "")
BASE_ID      = "appi9PUu4ZqKiOXkw"
TABLE_ID     = "tblCWODP44zR22p8D"

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

def apify_reel_scraper(profile_url: str, results_limit: int = 200) -> list[dict]:
    """Use instagram-scraper (resultsType=reels) to get ALL reels from a profile."""
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(
        "https://api.apify.com/v2/acts/apify~instagram-scraper/runs",
        headers=headers,
        json={
            "directUrls": [profile_url],
            "resultsType": "reels",
            "resultsLimit": results_limit,
            "skipPinnedPosts": False,
        },
        timeout=30,
    )
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    print(f"    Apify run: {run_id}")

    for i in range(60):
        time.sleep(10)
        sr = requests.get(
            f"https://api.apify.com/v2/acts/apify~instagram-reel-scraper/runs/{run_id}",
            headers=headers, timeout=15
        )
        sr.raise_for_status()
        status = sr.json()["data"]["status"]
        if i % 3 == 0:
            print(f"    [{i * 10}s] {status}")
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify run ended: {status}")
    else:
        raise TimeoutError("Apify timed out after 10 minutes")

    ds = sr.json()["data"]["defaultDatasetId"]
    ir = requests.get(
        f"https://api.apify.com/v2/datasets/{ds}/items?limit={results_limit * 2}",
        headers=headers, timeout=60
    )
    ir.raise_for_status()
    items = ir.json()
    reels = []
    for x in items:
        sc = x.get("shortCode") or x.get("code") or ""
        if sc:
            reels.append(x)
    return reels


def at_headers() -> dict:
    return {"Authorization": f"Bearer {AIRTABLE_PAT}", "Content-Type": "application/json"}

def at_request(method: str, url: str, payload: dict = None) -> dict:
    for attempt in range(3):
        try:
            r = requests.request(method, url, headers=at_headers(), json=payload, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ReadTimeout as e:
            if attempt == 2:
                raise RuntimeError(f"Request timed out after 3 retries: {e}")
            wait = 2 ** attempt
            print(f"    Retry {attempt + 1} in {wait}s...")
            time.sleep(wait)

def at_fetch_all_ids() -> dict[str, str]:
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

def at_create(fields: dict) -> str:
    resp = at_request("post", f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}", {"fields": fields})
    if "id" not in resp:
        raise RuntimeError(f"Airtable create failed: {resp}")
    return resp["id"]

def week_number(d: date) -> str:
    return f"Week {d.isocalendar()[1]:02d}"

def post_type(t: str) -> str:
    if t in ("GraphVideo", "XDTGraphReel", "Video", "Reel"):
        return "REEL"
    if t in ("GraphSidecar", "Sidecar", "CAROUSEL"):
        return "CAROUSEL"
    return "PIC"


def main():
    parser = argparse.ArgumentParser(description="ALJ Reel Backfill — scrape all historical reels")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=200, help="Max reels per account (default 200)")
    parser.add_argument("--accounts", nargs="*", help="Specific accounts to scrape (default: all)")
    args = parser.parse_args()

    if not APIFY_TOKEN:
        sys.exit("ERROR: APIFY_TOKEN not set")
    if not AIRTABLE_PAT:
        sys.exit("ERROR: AIRTABLE_PAT not set")

    accounts = [a for a in ACCOUNTS if not args.accounts or a["username"].lower() in [x.lower() for x in args.accounts]]

    print(f"\n{'=' * 60}")
    print(f"ALJ REEL BACKFILL  --  {date.today()}")
    print(f"Accounts: {[a['username'] for a in accounts]}")
    print(f"Limit per account: {args.limit}")
    print(f"{'=' * 60}\n")

    index = at_fetch_all_ids()
    print(f"  Existing records: {len(index)}")

    total_new = 0
    total_skipped = 0
    total_errors = 0

    for account in accounts:
        name = account["name"]
        username = account["username"]
        profile_url = f"https://www.instagram.com/{username}/"
        print(f"\n[{name}] Scraping reels...")

        try:
            reels = apify_reel_scraper(profile_url, results_limit=args.limit)
        except Exception as e:
            print(f"  ERROR: {e}")
            total_errors += 1
            continue

        print(f"  {len(reels)} reels scraped")

        new_count = 0
        skipped_count = 0
        error_count = 0

        for reel in reels:
            sc = reel.get("shortCode") or reel.get("code") or ""
            if not sc:
                continue

            if sc in index:
                skipped_count += 1
                continue

            ts = reel.get("timestamp", "")
            try:
                posted_dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).date() if ts else date.today()
            except (ValueError, TypeError):
                posted_dt = date.today()

            views    = reel.get("videoPlayCount") or reel.get("videoViewCount") or 0
            likes    = reel.get("likesCount") or 0
            comments = reel.get("commentsCount") or 0

            fields = {
                "SHORTCODE":     sc,
                "ACCOUNT":       name,
                "POST URL":      f"https://www.instagram.com/p/{sc}/",
                "POST TYPE":     post_type(reel.get("type", "")),
                "DATE POSTED":   posted_dt.isoformat(),
                "WEEKDAY":       posted_dt.strftime("%A").upper(),
                "WEEK NO":       week_number(posted_dt),
                "LIKES":         likes,
                "COMMENTS":      comments,
                "VIEWS":         views,
                "PLAYS":         views,
                "SCRAPE STATUS": "OK",
                "SCRAPE DATE":   date.today().isoformat(),
            }

            if args.dry_run:
                print(f"  [DRY] CREATE {sc[:12]} ({name})")
                new_count += 1
            else:
                try:
                    at_create(fields)
                    print(f"  + {name} | {sc[:12]} | NEW")
                    new_count += 1
                except Exception as e:
                    print(f"  ERROR create {sc[:12]}: {e}")
                    error_count += 1

            time.sleep(0.25)

        print(f"  → {new_count} new, {skipped_count} already existed, {error_count} errors")
        total_new += new_count
        total_skipped += skipped_count
        total_errors += error_count

    print(f"\n{'=' * 60}")
    print("=== BACKFILL SUMMARY ===")
    print(f"  New records: {total_new}")
    print(f"  Already existed: {total_skipped}")
    print(f"  Errors: {total_errors}")
    print("===================")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
