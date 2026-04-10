#!/usr/bin/env python3
"""
scrape_to_airtable.py — Run daily scrape and write results to Airtable.
Wraps instagram-tracker/scrape.py with proper env sourcing.
"""
import subprocess, sys, os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ALJ_ROOT  = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPT_DIR)))  # alex-ofm/
ENV_FILE   = os.path.join(ALJ_ROOT, "scripts", "instagram-tracker", ".env")

# Load env vars so this script can also import from instagram-tracker
from dotenv import load_dotenv
load_dotenv(ENV_FILE)

def run():
    sys.path.insert(0, os.path.join(ALJ_ROOT, "scripts", "instagram-tracker"))
    import scrape as s

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--account", default=None)
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()

    target = [a for a in s.ACCOUNTS if not args.account or a["name"] == args.account]
    print(f"Target accounts: {[a['name'] for a in target]}")

    today = __import__("datetime").date.today()

    # ── Scrape ────────────────────────────────────────────────────────────────
    print(f"\nScraping {len(target)} profiles via Apify...")
    all_posts = s.scrape_profiles([a["url"] for a in target], s.RESULTS_LIMIT)

    # ── Restricted fallback ───────────────────────────────────────────────────
    restricted = [
        a for a in target
        if not any((p.get("ownerUsername") or "").lower() == a["name"].lower() for p in all_posts)
    ]
    fallback_posts = []
    if restricted:
        print(f"  Fallback for: {[a['name'] for a in restricted]}")
        for account in restricted:
            urls = s.at_get_recent_post_urls(account["name"], days_back=7)
            if urls:
                scraped = s.scrape_post_urls(list(dict.fromkeys(urls)))
                fallback_posts.extend(scraped)

    # Deduplicate
    seen = set()
    combined = []
    for p in all_posts + fallback_posts:
        sc = s.shortcode_from_url(p.get("url") or p.get("inputUrl") or "")
        if sc and sc not in seen:
            seen.add(sc)
            combined.append(p)

    # ── Write ────────────────────────────────────────────────────────────────
    total = 0
    for account in target:
        print(f"\n[{account['name']}]")
        n = s.process_account(account, combined, today, args.dry_run)
        total += n
        __import__("time").sleep(0.3)

    print(f"\n{'='*60}")
    print(f"✅ Done — {total} rows {'would be ' if args.dry_run else ''}written for {today}")
    print(f"{'='*60}")

if __name__ == "__main__":
    run()
