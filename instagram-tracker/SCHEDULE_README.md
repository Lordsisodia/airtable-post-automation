# Daily Scheduling — Instagram Tracker

## Files

- `com.alj-instagram-daily.plist` — LaunchAgent that runs `scrape.py` daily at 9:00 AM
- `scrape.py` — reads credentials from `.env` in the same directory (auto-loaded via `load_dotenv()`)

## Install

```bash
# Copy to LaunchAgents
cp com.alj-instagram-daily.plist ~/Library/LaunchAgents/

# Load (starts the scheduled job)
launchctl load ~/Library/LaunchAgents/com.alj-instagram-daily.plist

# Unload (to stop)
launchctl unload ~/Library/LaunchAgents/com.alj-instagram-daily.plist
```

## Verify

```bash
# See next scheduled run
launchctl list | grep alj

# Check logs
tail -f ~/Library/Logs/com.alj.iso-instagram-daily.log
# or the logs dir
tail -f /path/to/alex-ofm/logs/scrape.log
```

## Change schedule

Edit `StartCalendarInterval` in the plist:
- Daily at 9 AM: `Hour 9, Minute 0`
- Every 6 hours: use multiple `StartCalendarInterval` dicts
- After editing: `launchctl unload ... && launchctl load ...`

## Manual run (bypass schedule)

```bash
cd scripts/instagram-tracker
.venv/bin/python scrape.py --dry-run   # test
.venv/bin/python scrape.py             # real
```
