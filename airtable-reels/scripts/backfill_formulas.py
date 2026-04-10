#!/usr/bin/env python3
"""
backfill_formulas.py — Fill empty ENGAGEMENT RATE, PROFILE VISITS, REACH.
Uses Apify to backfill PROFILE VISITS and REACH for all shortcodes.

ENGAGEMENT RATE is computed directly: (LIKES + COMMENTS) / FOLLOWERS * 100
No API call needed — just PATCH each record.

PROFILE VISITS and REACH require re-scraping each post URL via Apify.
This is expensive (one Apify run per shortcode), so it's gated behind --with-visits.
"""
import os, sys, json, time, urllib.request, argparse
from statistics import median
from collections import defaultdict

PAT     = os.environ.get("AIRTABLE_PAT", "")
BASE    = "appi9PUu4ZqKiOXkw"
TABLE   = "tblCWODP44zR22p8D"
APIFY   = os.environ.get("APIFY_TOKEN", "")

# ── Helpers ────────────────────────────────────────────────────────────────────

def at_headers():
    return {"Authorization": f"Bearer {PAT}", "Content-Type": "application/json"}

def get_record(rec_id):
    url = f"https://api.airtable.com/v0/{BASE}/{TABLE}/{rec_id}"
    req = urllib.request.Request(url, headers=at_headers())
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())["fields"]

def patch_record(rec_id, fields, dry_run=True):
    if dry_run:
        print(f"  [DRY] PATCH {rec_id}: {fields}")
        return
    url = f"https://api.airtable.com/v0/{BASE}/{TABLE}/{rec_id}"
    data = json.dumps({"fields": fields}).encode()
    req = urllib.request.Request(url, data=data, headers=at_headers(), method="PATCH")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def fetch_all():
    all_recs, url = [], f"https://api.airtable.com/v0/{BASE}/{TABLE}?maxRecords=500"
    while url:
        req = urllib.request.Request(url, headers=at_headers())
        with urllib.request.urlopen(req) as r:
            page = json.loads(r.read())
        all_recs.extend(page["records"])
        url = f"https://api.airtable.com/v0/{BASE}/{TABLE}?offset={page['offset']}" if "offset" in page else None
    return all_recs

def apify_run_scraper(input_payload):
    import requests
    r = requests.post(
        f"https://api.apify.com/v2/acts/apify~instagram-scraper/runs",
        headers={"Authorization": f"Bearer {APIFY}", "Content-Type": "application/json"},
        json=input_payload, timeout=30,
    )
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    for i in range(60):
        time.sleep(10)
        sr = requests.get(f"https://api.apify.com/v2/acts/apify~instagram-scraper/runs/{run_id}",
                          headers={"Authorization": f"Bearer {APIFY}"}, timeout=15)
        status = sr.json()["data"]["status"]
        if i % 3 == 0:
            print(f"    [{i*10}s] {status}")
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify run ended: {status}")
    else:
        raise TimeoutError("Apify timed out")
    dataset_id = sr.json()["data"]["defaultDatasetId"]
    ir = requests.get(f"https://api.apify.com/v2/datasets/{dataset_id}/items?limit=100",
                      headers={"Authorization": f"Bearer {APIFY}"}, timeout=30)
    return ir.json() if isinstance(ir.json(), list) else ir.json().get("items", [])

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--with-visits", action="store_true", help="Also backfill PROFILE VISITS / REACH via Apify (slow)")
    args = ap.parse()

    if not PAT:
        sys.exit("ERROR: AIRTABLE_PAT not set")

    print("Fetching all records...")
    recs = fetch_all()
    print(f"Total: {len(recs)} records")

    er_missing = [r for r in recs if not r["fields"].get("ENGAGEMENT RATE")]
    print(f"ENGAGEMENT RATE missing: {len(er_missing)}")

    # ── Backfill ENGAGEMENT RATE (free — compute from existing fields) ─────────
    print("\nBackfilling ENGAGEMENT RATE...")
    er_patched = 0
    for r in er_missing:
        f = r["fields"]
        likes    = f.get("LIKES") or 0
        comments = f.get("COMMENTS") or 0
        followers = f.get("FOLLOWERS AT SCRAPE") or 0
        if followers > 0 and f.get("SHORTCODE"):
            er = round((likes + comments) / followers * 100, 4)
            patch_record(r["id"], {"ENGAGEMENT RATE": str(er)}, dry_run=args.dry_run)
            er_patched += 1
    print(f"  ENGAGEMENT RATE filled: {er_patched}")

    # ── Backfill PROFILE VISITS / REACH (requires Apify re-scrape) ────────────
    if not args.with_visits:
        print("\nSkipping PROFILE VISITS / REACH (use --with-visits to enable)")
        return

    visits_missing = [r for r in recs if not r["fields"].get("PROFILE VISITS")]
    print(f"\nPROFILE VISITS missing: {len(visits_missing)}")

    # Group by account to batch-scrape
    by_acc = defaultdict(list)
    for r in visits_missing:
        by_acc[r["fields"].get("ACCOUNT","?")].append(r)

    total_visited = 0
    for acc, acc_recs in by_acc.items():
        urls = [r["fields"].get("POST URL") for r in acc_recs if r["fields"].get("POST URL")]
        if not urls:
            continue
        print(f"\n  Scraping {len(urls)} posts for {acc}...")
        scraped = apify_run_scraper({
            "directUrls": urls,
            "resultsType": "posts",
            "resultsLimit": len(urls),
            "addParentData": False,
        })
        scraped_map = {s.get("url", ""): s for s in scraped}
        for r in acc_recs:
            url = r["fields"].get("POST URL", "")
            if url in scraped_map:
                s = scraped_map[url]
                patch_record(r["id"], {
                    "PROFILE VISITS": s.get("profileVisitsCount") or s.get("ownerFollowersCount") or 0,
                    "REACH": s.get("reachCount") or 0,
                }, dry_run=args.dry_run)
                total_visited += 1
        time.sleep(2)

    print(f"\n✅ PROFILE VISITS / REACH patched: {total_visited}")

if __name__ == "__main__":
    main()
