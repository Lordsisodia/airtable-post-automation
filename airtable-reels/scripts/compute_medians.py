#!/usr/bin/env python3
"""
compute_medians.py — Compute per-account median LIKES and COMMENTS.
Stores results to median_stats.json — used by the benchmark score formula.
"""
import os, json, urllib.request
from collections import defaultdict
from statistics import median

PAT     = os.environ.get("AIRTABLE_PAT", "")
BASE    = "appi9PUu4ZqKiOXkw"
TABLE   = "tblCWODP44zR22p8D"

def fetch_all():
    all_recs = []
    url = f"https://api.airtable.com/v0/{BASE}/{TABLE}?maxRecords=500"
    while url:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {PAT}"})
        with urllib.request.urlopen(req) as resp:
            page = json.loads(resp.read())
        all_recs.extend(page["records"])
        url = f"https://api.airtable.com/v0/{BASE}/{TABLE}?offset={page['offset']}" if "offset" in page else None
    return all_recs

def compute():
    recs = fetch_all()
    by_acc = defaultdict(list)
    for r in recs:
        f = r["fields"]
        likes    = f.get("LIKES") or 0
        comments = f.get("COMMENTS") or 0
        if f.get("SHORTCODE") and likes > 0:  # exclude ghost records
            by_acc[f.get("ACCOUNT","?")].append((likes, comments))

    stats = {}
    print(f"{'Account':20s}  {'Median Likes':13s}  {'Median Cmts':13s}  {'Posts':6s}")
    print("-" * 58)
    for acc in sorted(by_acc.keys()):
        posts = by_acc[acc]
        likes_list    = sorted([p[0] for p in posts])
        comments_list = sorted([p[1] for p in posts])
        ml = median(likes_list)    if likes_list    else 0
        mc = median(comments_list)  if comments_list else 0
        stats[acc] = {"median_likes": ml, "median_comments": mc, "count": len(posts)}
        print(f"{acc:20s}  {ml:13.1f}  {mc:13.1f}  {len(posts):6d}")

    out = os.path.join(os.path.dirname(__file__), "median_stats.json")
    with open(out, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"\nSaved to {out}")
    return stats

if __name__ == "__main__":
    if not PAT:
        print("ERROR: AIRTABLE_PAT not set")
        sys.exit(1)
    compute()
