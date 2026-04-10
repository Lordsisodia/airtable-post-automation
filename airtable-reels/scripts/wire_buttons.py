#!/usr/bin/env python3
"""
wire_buttons.py — Populate OPEN REEL / OPEN PIC button fields from POST URL.
OPEN PROFILE is already configured. This fills the three null buttons.

Logic:
  POST TYPE = REEL      → OPEN REEL  = POST URL
  POST TYPE = PIC        → OPEN PIC   = POST URL
  POST TYPE = CAROUSEL   → OPEN REEL  = POST URL, OPEN REEL 2 = POST URL
  (OPEN PROFILE is pre-configured)
"""
import os, sys, json, urllib.request, argparse

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

def patch_record(rec_id, fields, dry_run=True):
    if dry_run:
        print(f"  [DRY] PATCH {rec_id}: {fields}")
        return
    url = f"https://api.airtable.com/v0/{BASE}/{TABLE}/{rec_id}"
    data = json.dumps({"fields": fields}).encode()
    req = urllib.request.Request(url, data=data, headers=at_headers(), method="PATCH")
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  ERROR PATCH {rec_id}: {e.code} — {body[:200]}")
        raise

def wire_buttons(dry_run=True):
    recs = fetch_all()
    print(f"Total records: {len(recs)}")

    reel_wired = pic_wired = already_wired = skipped = 0
    errors = 0

    for r in recs:
        f = r["fields"]
        post_url  = f.get("POST URL", "")
        post_type = f.get("POST TYPE", "").upper()
        rec_id    = r["id"]

        # Current button state
        open_reel  = f.get("OPEN REEL",  {})
        open_reel2 = f.get("OPEN REEL 2", {})
        open_pic   = f.get("OPEN PIC",   {})

        if not post_url:
            skipped += 1
            continue

        updates = {}

        if post_type == "REEL":
            if open_reel and open_reel.get("url"):
                already_wired += 1
            else:
                updates["OPEN REEL"] = {"label": "OPEN REEL", "url": post_url}
                reel_wired += 1

        elif post_type == "PIC":
            if open_pic and open_pic.get("url"):
                already_wired += 1
            else:
                updates["OPEN PIC"] = {"label": "OPEN PIC", "url": post_url}
                pic_wired += 1

        elif post_type == "CAROUSEL":
            if open_reel and not open_reel.get("url"):
                updates["OPEN REEL"] = {"label": "OPEN REEL", "url": post_url}
                reel_wired += 1
            if open_reel2 and not open_reel2.get("url"):
                updates["OPEN REEL 2"] = {"label": "OPEN REEL", "url": post_url}
                reel_wired += 1
            if not updates:
                already_wired += 1

        else:
            # Unknown type — try reel button as fallback
            if post_url and (not open_reel or not open_reel.get("url")):
                updates["OPEN REEL"] = {"label": "OPEN REEL", "url": post_url}
                reel_wired += 1

        if updates:
            try:
                patch_record(rec_id, updates, dry_run=dry_run)
            except Exception:
                errors += 1

    print(f"\n{'='*60}")
    print(f"  Reel buttons wired:    {reel_wired}")
    print(f"  Pic buttons wired:     {pic_wired}")
    print(f"  Already wired:        {already_wired}")
    print(f"  Skipped (no URL):      {skipped}")
    print(f"  Errors:                {errors}")
    print(f"{'='*60}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--execute", dest="dry_run", action="store_false")
    args = ap.parse_args()
    if not PAT:
        sys.exit("ERROR: AIRTABLE_PAT not set")
    wire_buttons(dry_run=args.dry_run)
