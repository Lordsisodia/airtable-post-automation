#!/usr/bin/env python3
"""read_schedule.py — Read POSTS SCHEDULE table."""
import os, json, urllib.request

PAT     = os.environ.get("AIRTABLE_PAT", "")
BASE    = "appi9PUu4ZqKiOXkw"
TABLE   = "tblmH8ne5KSdRiBLJ"

def fetch_all():
    all_recs, url = [], f"https://api.airtable.com/v0/{BASE}/{TABLE}?maxRecords=500"
    while url:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {PAT}"})
        with urllib.request.urlopen(req) as r:
            page = json.loads(r.read())
        all_recs.extend(page["records"])
        url = f"https://api.airtable.com/v0/{BASE}/{TABLE}?offset={page['offset']}" if "offset" in page else None
    return all_recs

def main():
    if not PAT:
        print("ERROR: AIRTABLE_PAT not set")
        return
    recs = fetch_all()
    print(f"Total schedule entries: {len(recs)}")
    for r in recs:
        f = r["fields"]
        print(f"\n  [{r['id']}]")
        print(f"    Date: {f.get('Session Date','?')}")
        print(f"    Accounts: {f.get('ACCOUNT','?')}")
        print(f"    Model: {f.get('MODEL','?')}")
        print(f"    Status: {f.get('Daily Done','?')}")
        print(f"    Reels: {f.get('REELS','?')}")
        print(f"    Pics: {f.get('PICS','?')}")

if __name__ == "__main__":
    main()
