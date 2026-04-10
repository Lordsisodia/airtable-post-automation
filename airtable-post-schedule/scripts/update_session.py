#!/usr/bin/env python3
"""update_session.py — Update a posting session (mark done, add reel links, etc.)."""
import os, json, urllib.request, argparse

PAT     = os.environ.get("AIRTABLE_PAT", "")
BASE    = "appi9PUu4ZqKiOXkw"
TABLE   = "tblmH8ne5KSdRiBLJ"

def at_headers():
    return {"Authorization": f"Bearer {PAT}", "Content-Type": "application/json"}

def patch_record(rec_id, fields, dry_run=True):
    if dry_run:
        print(f"  [DRY] PATCH {rec_id}: {fields}")
        return
    url = f"https://api.airtable.com/v0/{BASE}/{TABLE}/{rec_id}"
    data = json.dumps({"fields": fields}).encode()
    req = urllib.request.Request(url, data=data, headers=at_headers(), method="PATCH")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--record-id", required=True)
    ap.add_argument("--status", choices=["NOT STARTED","IN PROGRESS","DONE"])
    ap.add_argument("--reels", default=None)
    ap.add_argument("--pics", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", dest="dry_run", action="store_false")
    args = ap.parse_args()
    if not PAT:
        print("ERROR: AIRTABLE_PAT not set")
        return
    fields = {}
    if args.status: fields["Daily Done"] = args.status
    if args.reels:  fields["REELS"] = args.reels
    if args.pics:   fields["PICS"] = args.pics
    patch_record(args.record_id, fields, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
