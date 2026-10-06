# Horse Body Weight Dashboard

A Streamlit dashboard for monitoring whether horse body weight data is available for upcoming and past races in Hong Kong and Japan. The app checks multiple sources of truth and presents a color-coded status so users can quickly see whether body weight data is available from Analytics, DMT, local scraped results, or is missing entirely.

## What the dashboard does

The application reads race field data and evaluates body weight availability across these sources:

- Analytics S3 bucket
- DMT S3 bucket
- Local scraped results
- Base race field data from the internal parquet export

It then displays the results in a table with a simple status legend:

- 🔴 Missing / unavailable
- 🟠 Available from local scraped results
- 🟡 Available from DMT
- 🟢 Available from Analytics

## Features

- Monitor upcoming and past races for HK and JPN
- View a clear status summary with KPI-style metrics
- Auto-refresh the dashboard periodically
- Support both mounted NAS-based access and an S3-based implementation
- Runs as a containerized Streamlit app

## Project structure

- dashboard.py: Main Streamlit dashboard used in this repository
- dashboard_using_mounts.py: Alternate implementation using the same logic with mounted NAS paths
- dashboard_using_S3.py: S3-based implementation for checking Analytics data
- docker-compose.yml: Container orchestration for running the dashboard
- dockerfile: Container image definition
- requirements.txt: Python dependencies
- tests/: Unit tests for the S3-based helpers

## Requirements

- Python 3.10+
- Access to the internal data locations or appropriate credentials for S3 access
- A mounted NAS path at /mnt/nas for the default dashboard flow

## Setup

Create and activate a virtual environment, then install dependencies:

```bash
cd /home/covertai/workspaces/dashboard_project/horse_bodyweight_dashboard
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run locally

Start the Streamlit app:

```bash
streamlit run dashboard.py
```

Then open:

```text
http://localhost:8501
```

## Run with Docker Compose

```bash
docker compose up --build
```

The app will be available at:

```text
http://localhost:8501
```

## Data assumptions

The default dashboard expects access to parquet datasets under paths such as:

- /mnt/nas/production/central/production_s3fs_local/gtl_analytics_prod_eu_west_1_grd_004/parquet_export/fields
- /mnt/nas/production/covertai_local_results/prerace-data-live-system
- Internal S3-backed locations for DMT and Analytics data

If these resources are not available, the dashboard may show empty results or warnings.

## Testing

Run the test suite with:

```bash
pytest
```

## Notes

This project is intended for internal operational use and depends on internal data storage and access patterns. The dashboard is designed to be quickly deployable for monitoring race data availability rather than as a general-purpose analytics application.
