# Airtable Post Automation

Daily IG scrape → Airtable pipeline with benchmark scoring for OFM content performance tracking.

## What it does

Every day this pipeline:
1. Scrapes Instagram posts for configured accounts via Apify
2. Upserts results into Airtable (deduplicates by shortcode)
3. Computes per-account median likes/comments
4. Applies Point-ify benchmark scoring (grades A+ to F, badges)
5. Wires button fields to open posts

## Setup

### 1. Add GitHub Secrets

In your repo → **Settings → Secrets and variables → Actions**:

| Secret | Value |
|--------|-------|
| `AIRTABLE_PAT` | Your Airtable Personal Access Token |
| `APIFY_TOKEN` | Your Apify API token |

### 2. Push to GitHub

```bash
git init
git add .
git commit -m "feat: initial commit"
git remote add origin https://github.com/YOUR_USERNAME/airtable-post-automation.git
git push -u origin main
```

### 3. Run manually

Go to **Actions** tab → "Daily IG Scrape" → "Run workflow"

## Schedule

Edit `.github/workflows/scrape.yml` → `cron`. Defaults to 17:00 UTC (9am AEDT).

## Local / VPS

```bash
# Install
python3 -m venv .venv
source .venv/bin/activate
pip install -r instagram-tracker/requirements.txt

# Run manually
./run-scrape.sh
```

## Scripts

| Script | Purpose |
|--------|---------|
| `instagram-tracker/scrape.py` | Scrape IG via Apify → Airtable |
| `airtable-reels/scripts/compute_medians.py` | Per-account median stats |
| `airtable-reels/scripts/compute_benchmark.py` | Point-ify scoring → Airtable |
| `airtable-reels/scripts/wire_buttons.py` | Populate button URLs |
| `airtable-reels/scripts/read_records.py` | Read all records from Airtable |
| `airtable-reels/scripts/delete_ghost_records.py` | Remove null-shortcode records |
| `airtable-post-schedule/scripts/read_schedule.py` | Read POSTS SCHEDULE table |
| `airtable-post-schedule/scripts/create_session.py` | Create a posting session |
| `airtable-post-schedule/scripts/update_session.py` | Update session status |

## Credentials

Credentials are read from `.env` (gitignored) or set as GitHub Actions secrets. Never commit real credentials.
