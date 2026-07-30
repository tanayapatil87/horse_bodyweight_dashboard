"""#-----------------------------------------------------------------------------------------------
# This is a Streamlit dashboard to monitor the availability of horse body weight data for upcoming
# and past Races in Hong Kong and Japan.
# It checks multiple data sources (Analytics S3 bucket, DMT S3 bucket,  and local scraped results)
# to determine the availability status of body weight data.
# -----------------------------------------------------------------------------------------------
"""

#!/usr/bin/env python3

from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging

import polars as pl
import streamlit as st
from natsort import natsorted

from streamlit_autorefresh import st_autorefresh

# -------------------------------------------------------------------------------------------------
# Data at NAS locations :
#
# Basic race details(whole table contents except horse body weights which are later scrapped).
# We need race start time from this:
# /mnt/nas/production/central/production_s3fs_local/gtl_analytics_prod_eu_west_1_grd_004/
# parquet_export/fields
#
# 🔴 unavailable
#  	⬇ 	⬇
# 🟠 local results: /mnt/nas/production/covertai_local_results/prerace-data-live-system/
#  	⬇ 	⬇
# 🟡 dmt: /mnt/nas/covert_s3_bucket/covert_to_analytics_general_staging_gtl_s3_bucket/horses/
# merged_fields/
# 	⬇	⬇
# 🟢 analytics: /mnt/nas/covert_s3_bucket_may2025_batch/gtl_analytics_stage_eu_west_1_covert_001/
# horses/merged_fields/
# -------------------------------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.handlers.clear()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

CONFIG = {
    "directories": {
        "fields": (
            "/mnt/nas/production/central/production_s3fs_local/"
            "gtl_analytics_prod_eu_west_1_grd_004/"
            "parquet_export/fields"
        ),
        "local_results": (
            "/mnt/nas/production/covertai_local_results/" "prerace-data-live-system"
        ),
        "dmt": (
            "/mnt/nas/covert_s3_bucket/"
            "covert_to_analytics_general_staging_gtl_s3_bucket/"
        ),
        "analytics": (
            "/mnt/nas/covert_s3_bucket_may2025_batch/"
            "gtl_analytics_stage_eu_west_1_covert_001"
        ),
    },
    "lookahead_hours": 3,
}


# -----------------------------------------------------------------------------
# DATA LOADING
# -----------------------------------------------------------------------------


@st.cache_data(ttl=60)
def load_latest_fields():
    """
    Load latest field file and return races for HK/JPN.
    (on Analytics S3 bucket, so should be the most up-to-date)
    """

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    fields_dir = Path(CONFIG["directories"]["fields"])

    fields_files = natsorted(fields_dir.glob("**/*field*.parquet"))

    if not fields_files:
        return pl.DataFrame()

    latest_fields = fields_files[-1]

    df_latest_fields = (
        pl.scan_parquet(latest_fields)
        .unique("RaceKey")
        .select(
            [
                "RaceKey",
                "TrackCountry",
                "MeetingDate",
                "TrackID",
                "TrackName",
                "ToteIndicator",
                "RaceNo",
                "RaceStartTimeUTC",
                "RaceStartTime",
                "HorseBodyWeight",
            ]
        )
        .with_columns(
            (pl.col("RaceStartTimeUTC") - now)
            .dt.total_minutes()
            .alias("MinutesToStart"),
            (now - pl.col("RaceStartTimeUTC"))
            .dt.total_minutes()
            .alias("MinutesSinceFinish"),
        )
        .filter(
            pl.col.TrackCountry.is_in(("HK", "JPN")),
            (pl.col.RaceStartTimeUTC - now) <= pl.duration(hours=3),
        )
        .collect()
    )

    return df_latest_fields


@st.cache_data(ttl=60)
def check_local_results():
    """
    NAR scraped bodyweights.
    """

    local_dir = Path(CONFIG["directories"]["local_results"])

    latest_file = local_dir / "nar" / "outputs" / "latest.parquet"

    if not latest_file.exists():
        return pl.DataFrame(
            {
                "RaceKey": [],
                "HorseBodyWeightPresent": [],
            }
        )

    return (
        pl.scan_parquet(latest_file)
        .select(
            [
                ("FR" + pl.col("FieldRaceID").cast(pl.String)).alias("RaceKey"),
                (pl.count("HorseBodyWeight").over("FieldRaceID").gt(0)).alias(
                    "HorseBodyWeightPresent"
                ),
                (pl.col("LastUpdated").dt.strftime("%Y-%m-%d %H:%M:%S")).alias(
                    "TimeLatestUpdated"
                ),
            ]
        )
        .unique("RaceKey")
        .collect()
    )


@st.cache_data(ttl=60)
def analytics_available():
    """
    Check on Analytics S3 bucket for body weight availability.
    """

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    _now = now.replace(tzinfo=timezone.utc)

    latest_dir = f"/year={_now:%Y}" f"/month={_now:%m}" f"/day={_now:%d}" f"/"
    analytics_dir = Path(
        CONFIG["directories"]["analytics"] + "/horses/merged_fields" + latest_dir
    )

    fields_files = natsorted(analytics_dir.glob("**/*field*.parquet"))
    if not fields_files:
        return pl.DataFrame(
            {
                "RaceKey": [],
                "AnalyticsAvailable": [],
            }
        )

    analytics_fields = pl.scan_parquet(fields_files[-1])  # scan latest file

    return (
        analytics_fields.select(
            [
                pl.col("RaceKey"),
                (pl.col("HorseBodyWeight").any().over("RaceKey")).alias(
                    "AnalyticsAvailable"
                ),
            ]
        )
        .unique("RaceKey")
        .collect()
    )


def dmt_available():
    """
    Check on DMT S3 bucket for body weight availability.
    """

    now = datetime.now(timezone.utc).replace(tzinfo=None).replace(tzinfo=timezone.utc)

    latest_dir = f"/year={now:%Y}" f"/month={now:%m}" f"/day={now:%d}" f"/"
    dmt_dir = Path(CONFIG["directories"]["dmt"] + "/horses/merged_fields" + latest_dir)

    fields_files = natsorted(dmt_dir.glob("**/*field*.parquet"))
    if not fields_files:
        return pl.DataFrame(
            {
                "RaceKey": [],
                "DMTAvailable": [],
            }
        )

    dmt_fields = pl.scan_parquet(fields_files[-1])  # scan latest file

    return (
        dmt_fields.select(
            [
                pl.col("RaceKey"),
                (pl.col("HorseBodyWeight").any().over("RaceKey")).alias("DMTAvailable"),
            ]
        )
        .unique("RaceKey")
        .collect()
    )


def render_kpis(df_upcoming: pl.DataFrame):
    """Render KPIs for upcoming races based on body weight availability status."""

    analytics_count = df_upcoming.filter(pl.col("BodyWeightStatus") == "🟢").height
    dmt_count = df_upcoming.filter(pl.col("BodyWeightStatus") == "🟡").height
    scraped_count = df_upcoming.filter(pl.col("BodyWeightStatus") == "🟠").height
    missing_count = df_upcoming.filter(pl.col("BodyWeightStatus") == "🔴").height

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric(
        "Upcoming Races",
        len(df_upcoming),
    )

    col2.metric(
        "🟢",
        analytics_count,
    )

    col3.metric(
        "🟡",
        dmt_count,
    )

    col4.metric(
        "🟠",
        scraped_count,
    )

    col5.metric(
        "🔴",
        missing_count,
    )


# -----------------------------------------------------------------------------
# BUILD STATUS TABLE
# -----------------------------------------------------------------------------


@st.cache_data(ttl=60)
def build_dashboard_data():
    """Build the main dashboard dataframe by joining fields, local results,
    DMT availability, and Analytics availability.
    """

    df_fields = load_latest_fields()
    if len(df_fields) == 0:
        return df_fields

    df_analytics = analytics_available()
    df_dmt = dmt_available()
    df_local = check_local_results()

    logger.info("Fields: %s", False if df_fields.is_empty() else True)
    logger.info("Analytics: %s", False if df_analytics.is_empty() else True)
    logger.info("DMT: %s", False if df_dmt.is_empty() else True)
    logger.info("Local: %s\n", False if df_local.is_empty() else True)

    _df = (
        df_fields.join(
            df_local,
            on="RaceKey",
            how="left",
        )
        .join(
            df_analytics,
            on="RaceKey",
            how="left",
        )
        .join(
            df_dmt,
            on="RaceKey",
            how="left",
        )
        .with_columns(
            pl.col("AnalyticsAvailable").fill_null(False),
            pl.col("DMTAvailable").fill_null(False),
            pl.col("HorseBodyWeightPresent").fill_null(False),
            pl.col("TimeLatestUpdated").fill_null("N/A"),
        )
        .with_columns(
            pl.when(pl.col("AnalyticsAvailable"))
            .then(pl.lit("🟢"))
            .when(pl.col("DMTAvailable"))
            .then(pl.lit("🟡"))
            .when(pl.col("HorseBodyWeightPresent"))
            .then(pl.lit("🟠"))
            .otherwise(pl.lit("🔴"))
            .alias("BodyWeightStatus")
        )
    )

    df_dashboard = _df.with_columns(
        pl.when((pl.col("TrackCountry") == "JPN") & (pl.col("ToteIndicator") == "J"))
        .then(pl.lit("JRA"))
        .when((pl.col("TrackCountry") == "JPN") & (pl.col("ToteIndicator") == "N"))
        .then(pl.lit("NAR"))
        .otherwise(pl.col("TrackCountry"))
        .alias("Jurisdiction")
    )
    return df_dashboard


# -----------------------------------------------------------------------------
# STREAMLIT APP
# -----------------------------------------------------------------------------
_now = datetime.now(timezone.utc).replace(tzinfo=None)
st.set_page_config(
    page_title="Horse Racing Dashboard",
    page_icon="🏇",
    layout="wide",
)

st.logo("https://www.covert.jp/assets/img/ogp.png")
st.sidebar.title("CovertAI")

st.title("🏇 Horse Body Weight Monitoring")
# Refresh every 60 seconds
st_autorefresh(interval=60 * 1000, key="dashboard_refresh")

st.caption(f"Last refreshed: {_now.strftime('%Y-%m-%d %H:%M:%S')}")


with st.expander("About"):
    st.write("🔴 Horse body weight is not available at the source")
    st.write("🟠 Horse body weight is scraped and available at source")
    st.write("🟡 Horse body weight is available on DMT S3 bucket")
    st.write("🟢 Horse body weight is available at Analytics S3 bucket")
    st.write("Priority: 🟢 Analytics -> 🟡 DMT -> 🟠 Scraped -> 🔴 Missing")

df = build_dashboard_data()

if len(df) == 0:
    st.warning("No races found.")
    st.stop()

page = st.sidebar.radio(
    "Select Page",
    [
        "Upcoming Races",
        "Past Races",
    ],
)

if page == "Upcoming Races":

    lookahead = st.sidebar.slider(
        "Hours Ahead",
        min_value=1,
        max_value=12,
        value=CONFIG["lookahead_hours"],
    )

    upcoming = df.filter(
        (pl.col("RaceStartTimeUTC") >= _now)
        & (pl.col("RaceStartTimeUTC") <= _now + timedelta(hours=lookahead))
        & pl.col("TrackCountry").is_in(["HK", "JPN"])
    )

    st.subheader("Upcoming Races")

    st.dataframe(
        upcoming.select(
            [
                "TrackCountry",
                "Jurisdiction",
                "TrackName",
                "RaceNo",
                "RaceStartTimeUTC",
                "MinutesToStart",
                "BodyWeightStatus",
                "TimeLatestUpdated",
            ]
        )
        .sort("MinutesToStart")
        .to_pandas(),
        width="stretch",
        hide_index=True,
    )
    logger.info("Upcoming Races: %s", upcoming)
    st.subheader("Filters")

    jurisdictions = st.multiselect(
        "Jurisdiction",
        ["HK", "JRA", "NAR"],
        default=["HK", "JRA", "NAR"],
    )

    # -----------------------------------------------------------------------------
    # SUMMARY
    # -----------------------------------------------------------------------------

    upcoming_races_summary = (
        upcoming.group_by("BodyWeightStatus").len().sort("BodyWeightStatus")
    )

    st.subheader("Total")

    st.dataframe(
        upcoming_races_summary.to_pandas(),
        width="stretch",
        hide_index=True,
    )

else:

    past = df.filter(pl.col("RaceStartTimeUTC") < _now)

    st.subheader("Past Races")

    st.dataframe(
        past.select(
            [
                "TrackCountry",
                "Jurisdiction",
                "TrackName",
                "RaceNo",
                "RaceStartTimeUTC",
                "MinutesSinceFinish",
                "BodyWeightStatus",
                "TimeLatestUpdated",
            ]
        )
        .sort(
            "RaceStartTimeUTC",
            descending=True,
        )
        .to_pandas(),
        width="stretch",
        hide_index=True,
    )

    logger.info("Past Races: %s", past)
    st.subheader("Filters")

    jurisdictions = st.multiselect(
        "Jurisdiction",
        ["HK", "JRA", "NAR"],
        default=["HK", "JRA", "NAR"],
    )

    # -----------------------------------------------------------------------------
    # SUMMARY
    # -----------------------------------------------------------------------------

    past_races_summary = (
        past.group_by("BodyWeightStatus").len().sort("BodyWeightStatus")
    )

    st.subheader("Total")

    st.dataframe(
        past_races_summary.to_pandas(),
        width="stretch",
        hide_index=True,
    )

# -----------------------------------------------------------------------------
# SUMMARY
# -----------------------------------------------------------------------------

st.divider()

summary = df.group_by("BodyWeightStatus").len().sort("BodyWeightStatus")

st.subheader("All Races Summary")
st.caption("Summary of body weight availability for all upcoming and past races")

st.dataframe(
    summary.to_pandas(),
    width="stretch",
    hide_index=True,
)
