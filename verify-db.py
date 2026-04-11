#!/usr/bin/env python3
"""Verify Airtable record counts per account and post type."""
import os, requests

PAT = os.environ["AIRTABLE_PAT"]
BASE = "appi9PUu4ZqKiOXkw"
TABLE = "tblCWODP44zR22p8D"
H = {"Authorization": f"Bearer {PAT}"}

records = []
offset = None
while True:
    params = {"pageSize": 100, "fields[]": ["ACCOUNT", "POST TYPE", "POST URL"]}
    if offset:
        params["offset"] = offset
    url = f"https://api.airtable.com/v0/{BASE}/{TABLE}"
    r = requests.get(url, headers=H, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    records.extend(data.get("records", []))
    offset = data.get("offset")
    if not offset:
        break

accounts = {}
for rec in records:
    f = rec["fields"]
    acc = f.get("ACCOUNT", "UNKNOWN")
    pt = f.get("POST TYPE", "UNKNOWN")
    if acc not in accounts:
        accounts[acc] = {"total": 0, "types": {}}
    accounts[acc]["total"] += 1
    accounts[acc]["types"][pt] = accounts[acc]["types"].get(pt, 0) + 1

print(f"\nTotal records in Airtable: {len(records)}")
print(f"\n{'Account':<20} {'Total':>6}  Breakdown")
print("-" * 50)
for acc in sorted(accounts.keys()):
    d = accounts[acc]
    types_str = " | ".join(f"{k}:{v}" for k, v in sorted(d["types"].items()))
    print(f"{acc:<20} {d['total']:>6}  {types_str}")
