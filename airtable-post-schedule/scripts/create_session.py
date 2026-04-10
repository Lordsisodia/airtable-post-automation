#!/usr/bin/env python3
"""create_session.py — Create a new posting session in POSTS SCHEDULE."""
import os, json, urllib.request, argparse

PAT     = os.environ.get("AIRTABLE_PAT", "")
BASE    = "appi9PUu4ZqKiOXkw"
TABLE   = "tblmH8ne5KSdRiBLJ"

def at_headers():
    return {"Authorization": f"Bearer {PAT}", "Content-Type": "application/json"}

def create_session(date, accounts, model, dry_run=True):
    fields = {
        "Session Date": date,
        "WEEK DAY": __import__("datetime").date.fromisoformat(date).strftime("%A").upper(),
        "ACCOUNT": accounts,
        "MODEL": model,
        "Daily Done": "NOT STARTED",
    }
    if dry_run:
        print(f"  [DRY] CREATE: {fields}")
        return "dry-run-id"
    url = f"https://api.airtable.com/v0/{BASE}/{TABLE}"
    data = json.dumps({"fields": fields}).encode()
    req = urllib.request.Request(url, data=data, headers=at_headers())
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())["id"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="ISO date e.g. 2026-04-10")
    ap.add_argument("--accounts", required=True, help="Comma-separated account names")
    ap.add_argument("--model", default="ALEX")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", dest="dry_run", action="store_false")
    args = ap.parse_args()
    if not PAT:
        print("ERROR: AIRTABLE_PAT not set")
        return
    acc_list = [a.strip() for a in args.accounts.split(",")]
    for acc in acc_list:
        rid = create_session(args.date, acc, args.model, dry_run=args.dry_run)
        print(f"  → {rid}")

if __name__ == "__main__":
    main()
