---
name: airtable-reels
description: Read/write Performance Reels Database — primary analytics table for IG post performance tracking.
version: "1.0.0"
trigger: "/airtable-reels"
disable-model-invocation: false
user-invocable: true
context: agent
agent: general-purpose
allowed-tools: Bash
---

# airtable-reels — Performance Reels Database Tool

Read and write the **Performance Reels Database** (Airtable) for the 10-model IG tracker.

## Credentials

The `.env` in the `scripts/` directory holds:
```
APIFY_TOKEN=apify_api_...
AIRTABLE_PAT=pat...
BASE_ID=appi9PUu4ZqKiOXkw
TABLE_ID=tblCWODP44zR22p8D
```

## Quick Reference

**Table:** `tblCWODP44zR22p8D` (Performance Reels Database)
**Base:** `appi9PUu4ZqKiOXkw`

## Common Tasks

### Read all records (with stats)
```bash
cd skills/airtable-reels/scripts
python3 read_records.py
```

### Run daily scrape
```bash
cd skills/airtable-reels/scripts
python3 scrape_to_airtable.py --dry-run   # test first
python3 scrape_to_airtable.py              # real run
```

### Compute per-account medians (for benchmark)
```bash
python3 compute_medians.py
```

### Backfill empty fields (ENGAGEMENT RATE, PROFILE VISITS, REACH)
```bash
python3 backfill_formulas.py
```

### Delete ghost records (ELLA_ABG, REN.ABG null rows)
```bash
python3 delete_ghost_records.py --dry-run
python3 delete_ghost_records.py
```

## Field Reference

| Field | Type | Notes |
|-------|------|-------|
| SHORTCODE | text | Deduplication key |
| SCRAPE DATE | date | Day scrape was run |
| SCRAPE AGE | select | 24H / 48H / 72H / 96H / 120H / HISTORICAL |
| ACCOUNT | select | One of 10 accounts |
| LIKES / COMMENTS / VIEWS | number | Raw engagement |
| FOLLOWERS AT SCRAPE | number | At time of scrape |
| ENGAGEMENT RATE | formula | (LIKES + COMMENTS) / FOLLOWERS × 100 |
| PROFILE VISITS | number | Needs backfill via Apify |
| REACH | formula | Needs backfill |
| PLAYS | number | Synonym for VIEWS |

## Button Setup (Manual — UI Required)

`OPEN PROFILE` is already configured and working.

`OPEN REEL`, `OPEN REEL 2`, and `OPEN PIC` are **computed button fields** with no formula set — they return null. Fix in Airtable UI:

1. Click `OPEN REEL` column header → Field settings
2. Change button action → **Link to a URL field** → select `POST URL`
3. Do the same for `OPEN PIC` (it will show for PIC posts only)
4. For `OPEN REEL 2` → formula: `IF({POST TYPE}="CAROUSEL", {POST URL}, "")`

This is a 2-minute manual step. Cannot be done via API.

## Known Issues

1. **Duplicates:** Same shortcode scraped twice — Apify returns duplicates in one call. The dedup key is SHORTCODE + SCRAPE DATE + SCRAPE AGE. A cleanup dedup script exists.
2. **Ghost records:** ELLA_ABG and REN.ABG have rows with null shortcodes — delete these.
3. **RINXRENX/RIN_JAPAN518 followers=0:** Apify not returning followerCount for these accounts.
4. **ONLYTYLERREX:** Never returns data — account may be blocked/private.
