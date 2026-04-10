#!/usr/bin/env python3
"""
read_records.py — Read and summarize Performance Reels Database.
Fetches all pages, computes per-account stats, shows top posts, flags issues.
"""
import os, sys
from dotenv import load_dotenv
load_dotenv(__file__ + "/../.env")

import urllib.request, json
from collections import defaultdict

PAT      = os.environ["AIRTABLE_PAT"]
BASE_ID  = "appi9PUu4ZqKiOXkw"
TABLE_ID = "tblCWODP44zR22p8D"

def fetch_all(url):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {PAT}"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())

# Paginate
all_recs = []
url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}?maxRecords=500"
while url:
    page = fetch_all(url)
    all_recs.extend(page["records"])
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}?offset={page['offset']}" if "offset" in page else None

print(f"Total records: {len(all_recs)}")

by_acc = defaultdict(list)
for r in all_recs:
    f = r["fields"]
    by_acc[f.get("ACCOUNT","?")].append(f)

print()
print(f"{'Account':20s}  {'Posts':5s}  {'Last Scrape':12s}  {'Followers':9s}  {'Total Likes':11s}  {'Total Views':11s}")
print("-" * 78)
for acc in sorted(by_acc.keys()):
    posts = by_acc[acc]
    latest = max(posts, key=lambda x: x.get("SCRAPE DATE","0"))
    tv = sum(p.get("VIEWS",0) or 0 for p in posts)
    tl = sum(p.get("LIKES",0) or 0 for p in posts)
    fol = latest.get("FOLLOWERS AT SCRAPE") or 0
    print(f"{acc:20s}  {len(posts):5d}  {latest.get('SCRAPE DATE','?'):12s}  {fol:9,d}  {tl:11,d}  {tv:11,d}")

# Top posts
print()
print("=== TOP 10 POSTS BY VIEWS ===")
top = sorted([(r["fields"].get("VIEWS",0) or 0, r["fields"]) for r in all_recs], key=lambda x: x[0], reverse=True)[:10]
for views, f in top:
    if views <= 0:
        continue
    fol = f.get("FOLLOWERS AT SCRAPE") or 1
    er = round(((f.get("LIKES",0) or 0) + (f.get("COMMENTS",0) or 0)) / fol * 100, 2)
    print(f"  {f.get('ACCOUNT',''):20s}  {views:7,} views  er={er:6.2f}%  {f.get('SCRAPE AGE','?'):4s}  {f.get('SHORTCODE','')[:10]}")

# Ghost records
print()
print("=== GHOST RECORDS (null shortcode) ===")
for acc in ["ELLA_ABG", "REN.ABG"]:
    posts = [r["fields"] for r in all_recs if r["fields"].get("ACCOUNT") == acc]
    nulls = [p for p in posts if not p.get("SHORTCODE")]
    if nulls:
        print(f"  {acc}: {len(nulls)} ghost rows")
    else:
        print(f"  {acc}: OK ({len(posts)} real rows)")

# Duplicate shortcodes
print()
print("=== DUPLICATE SHORTCODE CHECK ===")
from collections import Counter
keys = [(f.get("SHORTCODE",""), f.get("SCRAPE DATE",""), f.get("SCRAPE AGE","")) for f in [r["fields"] for r in all_recs]]
dupes = [(k, c) for k, c in Counter(keys).items() if c > 1 and k[0]]
print(f"  {len(dupes)} duplicate key groups found")
for k, c in dupes[:5]:
    print(f"    {k}: {c} copies")
