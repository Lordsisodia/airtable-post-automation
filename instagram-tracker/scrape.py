#!/usr/bin/env python3
"""
ALJ Instagram Performance Tracker — Per-Post Snapshot Schema
Scrape latest posts from 10 model IG accounts and write one row per post
per snapshot to "Performance Reels Database".

Snapshot ages: 24H, 48H, 72H, 96H, 120H
Deduplication key: SHORTCODE + SCRAPE DATE + SCRAPE AGE

Usage:
  python scrape.py                          # daily mode, all accounts
  python scrape.py --dry-run                # fetch, print, don't write
  python scrape.py --account RINXRENX       # single account only
  python scrape.py --backfill                # re-scrape posts < 5 days old for velocity
"""

import os, sys, re, json, time, argparse
from datetime import date, timedelta, datetime, timezone
import requests
from dotenv import load_dotenv

load_dotenv()

# ── CREDENTIALS ────────────────────────────────────────────────────────────────

APIFY_TOKEN  = os.environ.get("APIFY_TOKEN", "")
AIRTABLE_PAT = os.environ.get("AIRTABLE_PAT", "")
BASE_ID      = "appi9PUu4ZqKiOXkw"
TABLE_ID     = "tblCWODP44zR22p8D"

# ── ACCOUNTS ───────────────────────────────────────────────────────────────────

ACCOUNTS = [
    {"name": "RIN_JAPAN518",   "url": "https://www.instagram.com/RIN_JAPAN518/"},
    {"name": "RINXRENX",       "url": "https://www.instagram.com/RINXRENX/"},
    {"name": "REN.ABG",        "url": "https://www.instagram.com/REN.ABG/"},
    {"name": "YOURELLAMIRA",   "url": "https://www.instagram.com/YOURELLAMIRA/"},
    {"name": "ELLA_ABG",       "url": "https://www.instagram.com/ELLA_ABG/"},
    {"name": "ELLAMOCHIMIRA_", "url": "https://www.instagram.com/ELLAMOCHIMIRA_/"},
    {"name": "ONLYREXFIT",     "url": "https://www.instagram.com/ONLYREXFIT/"},
    {"name": "_REXTYLER_",     "url": "https://www.instagram.com/_REXTYLER_/"},
    {"name": "ONLYTYLERREX",   "url": "https://www.instagram.com/ONLYTYLERREX/"},
    {"name": "ABG.RICEBUNNY",  "url": "https://www.instagram.com/abg.ricebunny/"},
]

RESULTS_LIMIT = 15
APIFY_ACTOR   = "apify~instagram-scraper"
MAX_SCRAPE_AGE_HOURS = 120   # only track posts up to 5 days old

# ── HELPERS ────────────────────────────────────────────────────────────────────

def shortcode_from_url(url: str) -> str:
    m = re.search(r"/(?:p|reel|tv)/([A-Za-z0-9_-]+)", url or "")
    return m.group(1) if m else ""

def week_number(d: date) -> str:
    return f"Week {d.isocalendar()[1]}"

def hours_old(post_timestamp: str) -> int | None:
    """Return hours between post timestamp and now, or None if timestamp is missing."""
    if not post_timestamp:
        return None
    try:
        posted = datetime.fromisoformat(post_timestamp.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - posted
        return int(delta.total_seconds() / 3600)
    except (ValueError, TypeError):
        return None

def scrape_age_label(hours: int) -> str:
    """Return 24H, 48H, 72H, 96H, or 120H for posts under 5 days old."""
    if hours <= 24:   return "24H"
    if hours <= 48:   return "48H"
    if hours <= 72:   return "72H"
    if hours <= 96:   return "96H"
    if hours <= 120:  return "120H"
    return None       # older than 5 days — skip

def calc_engagement_rate(likes: int, comments: int, followers: int) -> float:
    if not followers:
        return 0.0
    return round((likes + comments) / followers * 100, 2)

def post_type(post: dict) -> str:
    t = post.get("type", "")
    if t in ("GraphVideo", "XDTGraphReel", "Video", "Reel"):
        return "REEL"
    if t in ("GraphImage", "Image", "Photo"):
        return "PIC"
    if t in ("GraphSidecar", "Sidecar", "CAROUSEL"):
        return "CAROUSEL"
    # Fallback: infer from URL when type is null
    url = post.get("url") or post.get("inputUrl") or ""
    if "/reel/" in url or "/tv/" in url:
        return "REEL"
    if "/p/" in url:
        return "PIC"
    return "PIC"

def is_null_shell(post: dict) -> bool:
    return (
        post.get("type") is None
        and post.get("ownerUsername") is None
        and post.get("likesCount") is None
        and post.get("videoPlayCount") is None
    )

# ── APIFY ──────────────────────────────────────────────────────────────────────

def apify_headers():
    return {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}

def apify_run_scraper(input_payload: dict) -> list[dict]:
    r = requests.post(
        f"https://api.apify.com/v2/acts/{APIFY_ACTOR}/runs",
        headers=apify_headers(),
        json=input_payload,
        timeout=30,
    )
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    print(f"    Apify run: {run_id}")

    for i in range(60):
        time.sleep(10)
        sr = requests.get(
            f"https://api.apify.com/v2/acts/{APIFY_ACTOR}/runs/{run_id}",
            headers=apify_headers(),
            timeout=15,
        )
        sr.raise_for_status()
        status = sr.json()["data"]["status"]
        if i % 3 == 0:
            print(f"    [{i*10}s] {status}")
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify run {run_id} ended: {status}")
    else:
        raise TimeoutError(f"Apify run {run_id} timed out after 10 min")

    dataset_id = sr.json()["data"]["defaultDatasetId"]
    items_r = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items?limit=500",
        headers=apify_headers(),
        timeout=30,
    )
    items_r.raise_for_status()
    raw = items_r.json()
    items = raw if isinstance(raw, list) else raw.get("items", [])
    print(f"    Fetched {len(items)} results")
    return items

def scrape_profiles(profile_urls: list[str], limit: int = RESULTS_LIMIT) -> list[dict]:
    return apify_run_scraper({
        "directUrls": profile_urls,
        "resultsType": "posts",
        "resultsLimit": limit,
        "addParentData": True,
    })

def scrape_post_urls(post_urls: list[str]) -> list[dict]:
    return apify_run_scraper({
        "directUrls": post_urls,
        "resultsType": "posts",
        "resultsLimit": len(post_urls),
        "addParentData": True,
    })

# ── AIRTABLE ───────────────────────────────────────────────────────────────────

def at_headers():
    return {"Authorization": f"Bearer {AIRTABLE_PAT}", "Content-Type": "application/json"}

def at_find_row(shortcode: str, scrape_date: str, scrape_age: str) -> dict | None:
    """Find existing row by SHORTCODE + SCRAPE DATE + SCRAPE AGE."""
    formula = f"AND({{SHORTCODE}}='{shortcode}',{{SCRAPE DATE}}='{scrape_date}',{{SCRAPE AGE}}='{scrape_age}')"
    r = requests.get(
        f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}",
        headers=at_headers(),
        params={"filterByFormula": formula, "maxRecords": 1},
        timeout=15,
    )
    r.raise_for_status()
    records = r.json().get("records", [])
    return records[0] if records else None

def at_find_existing_snapshots(shortcode: str) -> dict:
    """Get all existing snapshot dates + ages for a shortcode (to avoid duplicate scrapes)."""
    formula = f"{{SHORTCODE}}='{shortcode}'"
    r = requests.get(
        f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}",
        headers=at_headers(),
        params={"filterByFormula": formula, "maxRecords": 50},
        timeout=15,
    )
    r.raise_for_status()
    records = r.json().get("records", [])
    return {(rec["fields"].get("SCRAPE DATE",""), rec["fields"].get("SCRAPE AGE","")): rec for rec in records}

def at_upsert(fields: dict, dry_run: bool) -> str:
    """Create or update a row. Returns record ID. Retries on timeout."""
    shortcode = fields.get("SHORTCODE", "")
    scrape_date = fields.get("SCRAPE DATE", "")
    scrape_age = fields.get("SCRAPE AGE", "")

    existing = at_find_row(shortcode, scrape_date, scrape_age)

    if dry_run:
        print(f"    [DRY] {'UPDATE' if existing else 'CREATE'}: {shortcode} {scrape_age} {scrape_date}")
        print(f"         likes={fields.get('LIKES')}, views={fields.get('VIEWS')}, followers={fields.get('FOLLOWERS AT SCRAPE')}")
        return existing["id"] if existing else "dry-run-id"

    method = "patch" if existing else "post"
    url = (f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}/{existing['id']}"
           if existing else
           f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}")
    payload = {"fields": fields}

    # Retry on timeout (Airtable occasionally times out under load)
    for attempt in range(3):
        try:
            r = requests.request(
                method, url,
                headers=at_headers(),
                json=payload,
                timeout=30,
            )
            r.raise_for_status()
            break
        except requests.exceptions.ReadTimeout:
            if attempt == 2:
                raise
            print(f"    Timeout, retrying ({attempt + 1}/3)...")
            time.sleep(2 ** attempt)

    if existing:
        print(f"    ✓ Updated {existing['id']}")
        return existing["id"]
    else:
        rec_id = r.json().get("id", "")
        print(f"    ✓ Created {rec_id}")
        return rec_id

def at_get_recent_post_urls(account_name: str, days_back: int = 7) -> list[str]:
    """Get all stored post URLs from recent rows — used as fallback for restricted accounts."""
    cutoff = (date.today() - timedelta(days=days_back)).isoformat()
    formula = (
        f"AND({{ACCOUNT}}='{account_name}',"
        f"IS_AFTER({{DATE POSTED}},'{cutoff}'),"
        f"OR({{OPEN REEL}}!='',{{OPEN REEL 2}}!='',{{OPEN PIC}}!=''))"
    )
    r = requests.get(
        f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}",
        headers=at_headers(),
        params={"filterByFormula": formula, "maxRecords": 20,
                "sort[0][field]": "DATE POSTED", "sort[0][direction]": "desc"},
        timeout=15,
    )
    r.raise_for_status()
    records = r.json().get("records", [])
    urls = []
    for rec in records:
        ef = rec["fields"]
        for field in ("OPEN REEL", "OPEN REEL 2", "OPEN PIC"):
            u = ef.get(field, "")
            if u and shortcode_from_url(u):
                urls.append(u)
    return list(dict.fromkeys(urls))

# ── FIELD BUILDER ──────────────────────────────────────────────────────────────

def build_fields(
    post: dict,
    account_name: str,
    profile_url: str,
    scrape_date: date,
    scrape_age: str,
    followers: int,
) -> dict:
    """Build Airtable fields dict for a single post snapshot."""
    likes    = post.get("likesCount") or 0
    comments = post.get("commentsCount") or 0
    views    = post.get("videoPlayCount") or post.get("videoViewCount") or 0
    plays    = views  # same value, separate field for clarity
    pt       = post_type(post)
    url      = post.get("url") or post.get("inputUrl") or ""

    # Posted timestamp
    ts = post.get("timestamp") or ""
    try:
        posted_dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).date() if ts else scrape_date
    except (ValueError, TypeError):
        posted_dt = scrape_date

    return {
        "SHORTCODE":           shortcode_from_url(url),
        "ACCOUNT":             account_name,
        "IG PROFILE URL":      profile_url,
        "POST URL":            url,
        "POST TYPE":           pt,
        "DATE POSTED":         posted_dt.isoformat(),
        "WEEKDAY":            posted_dt.strftime("%A").upper(),
        "WEEK NO":            week_number(posted_dt),
        "SCRAPE DATE":         scrape_date.isoformat(),
        "SCRAPE AGE":          scrape_age,
        "FOLLOWERS AT SCRAPE": followers,
        "LIKES":              likes,
        "COMMENTS":           comments,
        "VIEWS":              views,
        "PLAYS":              plays,
        "SCRAPE STATUS":       "OK",
    }

# ── PROCESSING ─────────────────────────────────────────────────────────────────

def process_account(account: dict, all_posts: list[dict], today: date, dry_run: bool,
                    existing_snapshots: dict = None) -> int:
    """
    For each post scraped for this account, write one row per applicable snapshot age.
    Returns number of rows written/updated.
    """
    name = account["name"]
    profile_url = account["url"]

    # Filter posts for this account
    posts = [
        p for p in all_posts
        if (p.get("ownerUsername") or "").lower() == name.lower()
    ]

    # Fallback: URL-based matching for restricted accounts
    if not posts:
        print(f"  ⚠  {name}: no profile posts, trying URL fallback...")
        # Would need access to stored URLs — handled at caller level
        return 0

    if is_null_shell(posts[0]):
        print(f"  ⚠  {name}: null shell returned, skipping")
        return 0

    followers = posts[0].get("followersCount") or 0
    rows_written = 0

    for post in posts:
        url = post.get("url") or post.get("inputUrl") or ""
        shortcode = shortcode_from_url(url)
        if not shortcode:
            continue

        # Determine post age
        ts = post.get("timestamp") or ""
        hrs = hours_old(ts)
        if hrs is None:
            print(f"    ⚠  no timestamp for {shortcode}, skipping")
            continue

        age_label = scrape_age_label(hrs)

        # Skip posts older than MAX_SCRAPE_AGE_HOURS
        if age_label is None:
            print(f"    → {shortcode}: {hrs}h old (>120h), skipping")
            continue

        # Skip if already scraped today for this age
        snap_key = (today.isoformat(), age_label)
        existing_map = existing_snapshots or {}
        if shortcode in existing_map:
            # Already processed this shortcode in this run — skip to avoid duplicates
            continue

        print(f"    {name} | {shortcode[:12]} | {hrs}h | {age_label} | followers={followers}")

        fields = build_fields(post, name, profile_url, today, age_label, followers)
        at_upsert(fields, dry_run)
        rows_written += 1

        # Track that we've processed this shortcode in this run
        if existing_snapshots is not None:
            existing_snapshots[shortcode] = True

    return rows_written

# ── MAIN ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ALJ Instagram per-post snapshot scraper")
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--account",  help="Single account to process")
    parser.add_argument("--backfill", action="store_true", help="Re-scrape posts < 5 days old from Airtable URLs")
    args = parser.parse_args()

    if not APIFY_TOKEN:
        sys.exit("ERROR: APIFY_TOKEN not set")
    if not AIRTABLE_PAT:
        sys.exit("ERROR: AIRTABLE_PAT not set")

    today = date.today()
    target = [a for a in ACCOUNTS if not args.account or a["name"] == args.account]

    if args.backfill:
        # Backfill: get post URLs from Airtable for recent posts, re-scrape them
        print(f"\n{'='*60}")
        print(f"BACKFILL MODE — re-scraping posts < 5 days old")
        print(f"{'='*60}\n")
        all_urls = []
        account_urls = {}
        for account in target:
            urls = at_get_recent_post_urls(account["name"], days_back=7)
            if urls:
                all_urls.extend(urls)
                account_urls[account["name"]] = urls
        if not all_urls:
            print("No URLs found to backfill")
            return
        print(f"Scraping {len(all_urls)} post URLs...")
        scraped = scrape_post_urls(all_urls)
        for account in target:
            acc_urls = account_urls.get(account["name"], [])
            acc_posts = [p for p in scraped if shortcode_from_url(p.get("url","")) in
                         [shortcode_from_url(u) for u in acc_urls]]
            existing = {}
            process_account(account, acc_posts, today, args.dry_run, existing)
        return

    # ── Normal daily mode ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"ALJ DAILY SCRAPE  —  {today}  —  {len(target)} accounts")
    if args.dry_run:
        print("DRY RUN — nothing will be written")
    print(f"{'='*60}\n")

    print(f"Scraping {len(target)} profiles...")
    all_posts = scrape_profiles([a["url"] for a in target], RESULTS_LIMIT)

    # ── Restricted account fallback ──────────────────────────────────
    restricted = [
        a for a in target
        if not any((p.get("ownerUsername") or "").lower() == a["name"].lower() for p in all_posts)
    ]

    fallback_posts = []
    if restricted:
        print(f"\n  Restricted: {[a['name'] for a in restricted]}")
        all_fallback_urls = []
        for account in restricted:
            urls = at_get_recent_post_urls(account["name"], days_back=7)
            print(f"    {account['name']}: {len(urls)} stored URLs found")
            all_fallback_urls.extend(urls)

        if all_fallback_urls:
            print(f"  Scraping {len(all_fallback_urls)} fallback URLs...")
            fallback_posts = scrape_post_urls(list(dict.fromkeys(all_fallback_urls)))
            print(f"  Fallback returned {len(fallback_posts)} posts")

    # Deduplicate by shortcode — profile scrape + fallback scrape can return same post
    seen: set = set()
    combined_posts: list = []
    for p in all_posts + fallback_posts:
        sc = shortcode_from_url(p.get("url") or p.get("inputUrl") or "")
        if sc and sc not in seen:
            seen.add(sc)
            combined_posts.append(p)

    total_rows = 0
    for account in target:
        print(f"\n[{account['name']}]")
        n = process_account(account, combined_posts, today, args.dry_run)
        total_rows += n
        time.sleep(0.3)

    print(f"\n{'='*60}")
    if args.dry_run:
        print(f"✅ Dry run complete — {total_rows} rows would be written")
    else:
        print(f"✅ Done — {total_rows} rows upserted for {today}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
