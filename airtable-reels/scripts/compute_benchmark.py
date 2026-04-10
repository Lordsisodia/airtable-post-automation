#!/usr/bin/env python3
"""
compute_benchmark.py — Compute benchmark scores for all posts in the Performance Reels Database.
Reads median_stats.json (from compute_medians.py), then PATCHes each record with:
  - BENCHMARK SCORE
  - BENCHMARK POINTS (0-1000)
  - GRADE
  - TIER
  - BADGES

Based on research/BENCHMARK_SYSTEM.md and research/SCORING_SYSTEM.md.
"""
import os, sys, json, urllib.request, math
from statistics import median
from collections import defaultdict

PAT     = os.environ.get("AIRTABLE_PAT", "")
BASE    = "appi9PUu4ZqKiOXkw"
TABLE   = "tblCWODP44zR22p8D"

# ── Credentials ────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, "..", ".env"))

# ── Helpers ────────────────────────────────────────────────────────────────────

def at_headers():
    return {"Authorization": f"Bearer {PAT}", "Content-Type": "application/json"}

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
        print(f"  ERROR PATCH {rec_id}: {e.code} {e.reason}: {body[:200]}")
        raise

def fetch_all():
    all_recs, url = [], f"https://api.airtable.com/v0/{BASE}/{TABLE}?maxRecords=500"
    while url:
        req = urllib.request.Request(url, headers=at_headers())
        with urllib.request.urlopen(req) as r:
            page = json.loads(r.read())
        all_recs.extend(page["records"])
        url = f"https://api.airtable.com/v0/{BASE}/{TABLE}?offset={page['offset']}" if "offset" in page else None
    return all_recs

def load_medians():
    path = os.path.join(SCRIPT_DIR, "median_stats.json")
    if not os.path.exists(path):
        print(f"WARNING: {path} not found — run compute_medians.py first")
        return {}
    with open(path) as f:
        return json.load(f)

# ── Benchmark Formula ──────────────────────────────────────────────────────────

def benchmark_score(likes, comments, followers, ml, mc):
    """Return (benchmark_score, likes_mult, comments_mult, er, viral_factor)."""
    # If followers is 0 but likes > 0, use global fallback follower estimate
    # so posts from zero-follower accounts can still score
    eff_followers = followers if followers > 0 else (likes * 50)  # rough ER-based proxy
    if ml <= 0:
        return 0.0, 0.0, 0.0, 0.0, 0.0

    likes_mult    = min(50, likes / ml)   if ml > 0 else 0
    comments_mult = min(50, comments / mc) if mc > 0 else 0
    er            = comments / likes       if likes > 0 else 0

    # Viral factor: log-scaled absolute performance
    max_likes = 20000  # approximate max in dataset
    viral_factor = (math.log10(likes + 1) / math.log10(max_likes)) if likes > 0 else 0

    # Engagement score: 0-1 from ER
    if   er < 0.01:  engagement_score = 0.0
    elif er < 0.03:  engagement_score = 0.3
    elif er < 0.08:  engagement_score = 0.6
    else:            engagement_score = 1.0

    bs = (
        0.40 * math.log(likes_mult + 1)    +
        0.25 * math.log(comments_mult + 1) +
        0.20 * engagement_score            +
        0.15 * viral_factor
    )
    return bs, likes_mult, comments_mult, er, viral_factor


def benchmark_points(bs):
    """Convert 0-4.2 score to 0-1000 points."""
    return min(1000, round(math.pow(bs, 1.15) * 215 + 20))


# Percentile-based thresholds calibrated from 424-record dataset (2026-04-10).
# Original fixed thresholds (900/850/800...) were designed for scores up to ~960 pts.
# Dataset max is 450 pts, so all posts graded F — these fix that.
GRADE_MAP = [
    (403, "A+"), (251, "A"),  (240, "A-"),
    (183, "B+"), (170, "B"),  (146, "B-"),
    (107, "C+"), (91,  "C"),  (47,  "C-"),
    (42,  "D"),  (0,   "F"),
]

def grade(pts):
    for threshold, g in GRADE_MAP:
        if pts >= threshold:
            return g
    return "F"

TIER_MAP = {
    "A+": "S — Legendary", "A": "A — Strong", "A-": "A — Strong",
    "B+": "B — Solid",    "B": "B — Solid",  "B-": "B — Solid",
    "C+": "C — Marginal",  "C": "C — Marginal", "C-": "C — Marginal",
    "D":  "D — Weak",      "F":  "F — Reject",
}

def badges(likes_mult, comments_mult, er, likes, pts):
    b = []
    if likes_mult >= 10:  b.append("10x Like Storm")
    if comments_mult >= 10: b.append("10x Comment Storm")
    if er >= 0.15:        b.append("ER Legend")
    elif er >= 0.08:       b.append("ER Beast")
    if likes >= 50000:     b.append("Viral 50k")
    if pts >= 900:          b.append("S — Legendary")
    elif pts >= 800:       b.append("A — Strong")
    # Double 10x suppress individual
    if likes_mult >= 10 and comments_mult >= 10:
        b = [x for x in b if x not in ("10x Like Storm", "10x Comment Storm")]
        b.insert(0, "Double 10x")
    return b

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--execute", dest="dry_run", action="store_false")
    args = ap.parse_args()

    if not PAT:
        sys.exit("ERROR: AIRTABLE_PAT not set")

    medians = load_medians()
    if not medians:
        print("No median stats — run compute_medians.py first")
        return

    recs = fetch_all()
    print(f"Records: {len(recs)}")

    # Only compute benchmark for 120H snapshots (mature, final scores).
    # Skip 24H/48H/72H/96H intermediate snapshots.
    recs_120h = [r for r in recs if r["fields"].get("SCRAPE AGE") == "120H"]
    print(f"120H snapshots: {len(recs_120h)}")

    updated = 0
    for r in recs_120h:
        f = r["fields"]
        likes    = f.get("LIKES") or 0
        comments = f.get("COMMENTS") or 0
        followers = f.get("FOLLOWERS AT SCRAPE") or 0
        shortcode = f.get("SHORTCODE", "")

        if not shortcode or likes <= 0:
            continue

        acc = f.get("ACCOUNT", "?")
        m = medians.get(acc, {"median_likes": 485, "median_comments": 9})
        ml, mc = m["median_likes"], m["median_comments"]

        bs, lm, cm, er, vf = benchmark_score(likes, comments, followers, ml, mc)
        pts = benchmark_points(bs)
        g   = grade(pts)
        tier = TIER_MAP.get(g, "?")
        b   = badges(lm, cm, er, likes, pts)

        # Compute ENGAGEMENT_RATE: (likes + comments) / followers
        er = (likes + comments) / followers if followers > 0 else 0.0

        fields = {
            "BENCHMARK SCORE":   round(bs, 4),
            "BENCHMARK POINTS": pts,
            "GRADE":            g,
            "TIER":             tier,
            "BADGES":           "; ".join(b) if b else "",
            "ENGAGEMENT_RATE":  round(er, 6),
        }

        patch_record(r["id"], fields, dry_run=args.dry_run)
        updated += 1

    print(f"\n{'='*60}")
    if args.dry_run:
        print(f"✅ Dry run — {updated} records would be updated")
    else:
        print(f"✅ Updated {updated} records")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
