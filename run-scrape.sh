#!/usr/bin/env bash
# run-scrape.sh — Run daily IG scrape + benchmark + wire buttons
# Usage: ./run-scrape.sh [--dry-run]
# Needs env vars: AIRTABLE_PAT, APIFY_TOKEN

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SKILLS_DIR="$PROJECT_ROOT/skills/airtable-reels/scripts"

DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN="--dry-run"
fi

echo "============================================================"
echo "ALJ Daily IG Scrape  —  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"

# ── Step 1: Scrape ──────────────────────────────────────────────────────────
echo ""
echo "[1/4] Running Apify scrape..."
cd "$SCRIPT_DIR"
if [[ -n "$DRY_RUN" ]]; then
  python3 scrape.py --dry-run
else
  python3 scrape.py
fi

# ── Step 2: Compute medians ──────────────────────────────────────────────────
echo ""
echo "[2/4] Computing per-account medians..."
cd "$SKILLS_DIR"
python3 compute_medians.py

# ── Step 3: Benchmark scores ─────────────────────────────────────────────────
echo ""
echo "[3/4] Computing benchmark scores..."
if [[ -n "$DRY_RUN" ]]; then
  python3 compute_benchmark.py --dry-run
else
  python3 compute_benchmark.py --execute
fi

# ── Step 4: Wire buttons ─────────────────────────────────────────────────────
echo ""
echo "[4/4] Wiring button URLs..."
if [[ -n "$DRY_RUN" ]]; then
  python3 wire_buttons.py --dry-run
else
  python3 wire_buttons.py --execute
fi

echo ""
echo "============================================================"
echo "Done  —  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
