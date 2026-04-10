---
name: airtable-post-schedule
description: Read and write the POSTS SCHEDULE table — plans daily posting sessions across IG accounts.
version: "1.0.0"
trigger: "/airtable-post-schedule"
disable-model-invocation: false
user-invocable: true
context: agent
agent: general-purpose
allowed-tools: Bash
---

# airtable-post-schedule — Posting Schedule Manager

Manages the `POSTS SCHEDULE` table (tblmH8ne5KSdRiBLJ) in the Airtable base.

## Credentials

```
AIRTABLE_PAT=<your-pat-here>
BASE_ID=appi9PUu4ZqKiOXkw
TABLE_ID=tblmH8ne5KSdRiBLJ
```

## POSTS SCHEDULE Fields

| Field | Type | Description |
|-------|------|-------------|
| Session Date | date | Date of posting session |
| WEEK DAY | text | Day name (MONDAY, etc.) |
| ACCOUNTS | select | Which accounts posting today |
| Assignee | collaborator | Who is responsible |
| REELS | url | Link to scheduled reels |
| PICS | url | Link to scheduled pics |
| Pics Posted | checkbox | Mark when done |
| Daily Done | select | NOT STARTED / IN PROGRESS / DONE |
| MODEL | select | Which model |
| IG PROFILE URL | text | Profile link |

## Scripts

### Read schedule
```bash
cd skills/airtable-post-schedule/scripts
python3 read_schedule.py
```

### Create a posting session
```bash
python3 create_session.py --date 2026-04-10 --accounts "ABG.RICEBUNNY,ELLAMOCHIMIRA_" --model "ALEX"
```

### Mark session complete
```bash
python3 update_session.py --record-id recXXXX --status DONE
```

### List all pending sessions
```bash
python3 list_pending.py
```
