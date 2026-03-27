# ETL Failure Auto-Fix Bot

An AI-powered Streamlit app built on Snowflake to detect, analyze, review, and help resolve ETL pipeline failures. The app uses Snowflake Cortex with `mistral-large2` to diagnose issues, suggest SQL-based fixes, support human approval workflows, and send notification emails.

## Overview

ETL Failure Auto-Fix Bot helps data teams reduce downtime by:

- ingesting failed ETL query history
- analyzing failures with AI
- suggesting root causes and SQL fixes
- requiring human approval before applying fixes
- tracking fix history and outcomes
- enabling natural-language Q&A over ETL incidents
- sending email notifications for unresolved failures

## Features

### 1. Dashboard
- View total failures, analyzed failures, critical incidents, and auto-fixed cases
- Explore:
  - failures by category
  - failures by severity
  - hourly failure timeline
- Trigger:
  - query history ingestion
  - re-analysis of failures

### 2. Failure Details
- Browse ETL failures with AI diagnosis
- Review:
  - root cause
  - suggested fix
  - severity
  - confidence score
  - generated SQL fix
- Approve and apply fixes manually

### 3. Auto-Fix Results
- Review recent fix attempts
- Track:
  - successful fixes
  - failed fixes
  - blocked fixes

### 4. Fix History & Settings
- View historical fix activity
- Inspect auto-fix settings stored in Snowflake tables

### 5. Talk to Fix Bot
- Ask natural language questions about ETL failures and fixes
- Responses are generated using Snowflake Cortex based on current ETL data

### 6. Notifications
- Configure email notification integration
- Send:
  - test notifications
  - unresolved failure summary emails

---

## Tech Stack

- **Frontend:** Streamlit
- **Backend / Data Platform:** Snowflake
- **AI Model:** Snowflake Cortex `mistral-large2`
- **Python Libraries:**
  - `streamlit`
  - `snowflake-snowpark-python`
- **Execution Environment:** Snowflake Streamlit app / Snowpark session

---

## Architecture

The app connects directly to an active Snowflake session and interacts with ETL monitoring tables and stored procedures.

### Core Snowflake Objects Used

#### Tables
- `ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS`
- `ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS`
- `ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY`
- `ETL_BOT.ETL_SCHEMA.AUTO_FIX_SETTINGS`

#### Stored Procedures / Functions
- `ETL_BOT.ETL_SCHEMA.ANALYZE_NEW_FAILURES()`
- `ETL_BOT.ETL_SCHEMA.INGEST_FROM_QUERY_HISTORY()`
- `SNOWFLAKE.CORTEX.COMPLETE(...)`
- `SYSTEM$SEND_EMAIL(...)`

---

## How It Works

1. The app loads ETL failure records from Snowflake.
2. Unanalyzed failures are sent to a stored procedure for AI diagnosis.
3. Suggested fixes are displayed to users for review.
4. A user can approve a fix and execute the generated SQL.
5. Results are logged into fix history.
6. Users can query the bot in plain English for insights.
7. Teams can send notification emails for unresolved failures.

---

## Project Structure

```text
.
├── app.py                  # Main Streamlit application
├── README.md               # Project documentation
└── requirements.txt        # Python dependencies
