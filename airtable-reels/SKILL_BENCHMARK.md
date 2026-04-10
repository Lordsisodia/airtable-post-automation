---
name: airtable-benchmark
description: Compute benchmark scores and grades for all posts in the Performance Reels Database.
version: "1.0.0"
trigger: "/airtable-benchmark"
disable-model-invocation: false
user-invocable: true
context: agent
agent: general-purpose
allowed-tools: Bash
---

# airtable-benchmark — Benchmark Scoring for IG Posts

Computes per-post benchmark scores using the **Point-ify** system defined in `research/SCORING_SYSTEM.md`.

## Pre-requisites

1. Run `compute_medians.py` first (computes per-account median likes/comments)
2. Results saved to `median_stats.json` in the same `scripts/` directory

## Workflow

```bash
# Step 1 — compute per-account medians
cd skills/airtable-reels/scripts
python3 compute_medians.py

# Step 2 — compute benchmark scores
python3 compute_benchmark.py --dry-run   # preview
python3 compute_benchmark.py --execute   # write to Airtable
```

## What Gets Written

| Field | Description |
|-------|-------------|
| BENCHMARK SCORE | Composite 0-4.2 score (log-weighted) |
| BENCHMARK POINTS | Normalized 0-1000 score |
| GRADE | A+ / A / A- / B+ / B / B- / C+ / C / C- / D / F |
| TIER | S-Legendary / A-Strong / B-Solid / C-Marginal / D-Weak / F-Reject |
| BADGES | Earned badges (10x Like Storm, ER Beast, etc.) |

## Grade Thresholds

| Grade | Points | Tier |
|-------|--------|------|
| A+ | 900+ | S — Legendary |
| A | 850-899 | A — Strong |
| A- | 800-849 | A — Strong |
| B+ | 750-799 | B — Solid |
| B | 700-749 | B — Solid |
| B- | 650-699 | B — Solid |
| C+ | 600-649 | C — Marginal |
| C | 550-599 | C — Marginal |
| C- | 500-549 | C — Marginal |
| D | 400-499 | D — Weak |
| F | < 400 | F — Reject |

## Badges

- **10x Like Storm** — likes 10x above account median
- **10x Comment Storm** — comments 10x above account median
- **ER Beast** — ER ≥ 8% (likers who commented)
- **ER Legend** — ER ≥ 15%
- **Viral 50k** — ≥ 50,000 likes
- **Double 10x** — both likes and comments 10x+ (suppresses individual storm badges)

## Formula

```
benchmark_score = (
    0.40 × log(likes_multiplier) +
    0.25 × log(comments_multiplier) +
    0.20 × engagement_score +
    0.15 × viral_factor
)
benchmark_points = min(1000, round(benchmark_score ^ 1.15 × 215 + 20))
```
