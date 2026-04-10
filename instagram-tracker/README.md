# Instagram Performance Tracker

Automated pipeline that scrapes Instagram posts from 10 OFM accounts, stores 3-day performance data in Airtable, and runs twice daily via GitHub Actions.

## Architecture

```
GitHub Actions (6am & 6pm UTC)
  └─ scrape.py
       ├─ Apify → instagram-post-scraper (basicData, all 10 accounts in 1 call)
       └─ Airtable → upsert (update existing rows or create new ones)

Airtable tblCWODP44zR22p8D ("ALJ Model Grades")
  └─ compute_medians.py + compute_benchmark.py → BENCHMARK SCORE, GRADE, TIER
```

## Accounts

| Account | Status |
|---------|--------|
| abg.ricebunny | Working |
| yourellamira | Working |
| ella_abg | Working |
| ellachimirira_ | Working |
| onlyrexfit | Working |
| _rextyler_ | Working |
| onlytylerrex | Working |
| RIN_JAPAN518 | **Restricted** |
| RINXRENX | **Restricted** |
| REN.ABG | **Restricted** |

## Airtable Field Mapping

| Apify Field | Airtable Field |
|-------------|----------------|
| shortCode | SHORTCODE |
| ownerUsername | ACCOUNT |
| timestamp | DATE POSTED |
| likesCount | LIKES |
| commentsCount | COMMENTS |
| videoPlayCount | VIEWS, PLAYS |
| type | POST TYPE |
| url | POST URL |
| — | SCRAPE STATUS = OK |
| — | SCRAPE DATE = today |
| — | WEEKDAY, WEEK NO |

## Running Locally

```bash
cd instagram-tracker

# Add credentials
cp .env.example .env
# Edit .env with APIFY_TOKEN and AIRTABLE_PAT

# Dry run
python scrape.py --dry-run

# Run for real
python scrape.py
```

## GitHub Actions Setup

1. Go to repo → Settings → Secrets → Actions
2. Add: `AIRTABLE_PAT` (Airtable PAT)
3. Add: `APIFY_TOKEN` (starts with `apify_api_`)
4. Workflow runs automatically at 6am and 6pm UTC

## Apify Costs

- Actor: `apify~instagram-post-scraper` with `dataDetailLevel: basicData`
- ~$0.0017 per post scraped
- 10 accounts × ~20 posts × 2 runs/day ≈ $0.68/day ≈ $20/month
