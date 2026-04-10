#!/usr/bin/env python3
"""
delete_ghost_records.py — Delete rows with null SHORTCODE (ELLA_ABG, REN.ABG).
These are failed scrape writes that should not exist in the database.
"""
import os, sys, json, urllib.request

PAT   = os.environ.get("AIRTABLE_PAT", "")
BASE  = "appi9PUu4ZqKiOXkw"
TABLE = "tblCWODP44zR22p8D"

def at_headers():
    return {"Authorization": f"Bearer {PAT}", "Content-Type": "application/json"}

def fetch_all():
    all_recs, url = [], f"https://api.airtable.com/v0/{BASE}/{TABLE}?maxRecords=500"
    while url:
        req = urllib.request.Request(url, headers=at_headers())
        with urllib.request.urlopen(req) as r:
            page = json.loads(r.read())
        all_recs.extend(page["records"])
        url = f"https://api.airtable.com/v0/{BASE}/{TABLE}?offset={page['offset']}" if "offset" in page else None
    return all_recs

def delete_record(rec_id, dry_run=True):
    if dry_run:
        print(f"  [DRY] DELETE {rec_id}")
        return
    url = f"https://api.airtable.com/v0/{BASE}/{TABLE}/{rec_id}"
    req = urllib.request.Request(url, headers=at_headers(), method="DELETE")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--execute", dest="dry_run", action="store_false")
    args = ap.parse_args()

    if not PAT:
        sys.exit("ERROR: AIRTABLE_PAT not set")

    print("Fetching records...")
    recs = fetch_all()

    ghosts = [r for r in recs if not r["fields"].get("SHORTCODE")]
    print(f"Ghost records found: {len(ghosts)}")
    for g in ghosts:
        f = g["fields"]
        print(f"  {g['id']}  account={f.get('ACCOUNT','?')}  created={g.get('createdTime','')[:10]}")

    if args.dry_run:
        print("\n(Dry run — pass --execute to delete)")
        return

    for g in ghosts:
        delete_record(g["id"], dry_run=False)
    print(f"\nDeleted {len(ghosts)} ghost records")

if __name__ == "__main__":
    main()
