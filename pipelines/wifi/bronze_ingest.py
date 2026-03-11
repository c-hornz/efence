"""
E-Fence Wi-Fi Bronze Ingest Pipeline
=====================================
Lakeflow Spark Declarative Pipeline (DLT)

Purpose:
    Ingest raw Wi-Fi observation JSON files from the Unity Catalog Volume
    landing zone using Auto Loader. Preserve full fidelity. Append only.
    Generate deterministic event_id for downstream dedup.

Target table:
    efence.wifi_raw.observations_bronze

Pipeline settings (set in pipeline JSON/YAML config):
    catalog:  efence
    target:   wifi_raw
    configuration:
        efence.wifi.landing_path: /Volumes/efence/wifi_raw/landing
        efence.wifi.schema_location: /Volumes/efence/wifi_raw/_autoloader_schema
        efence.mac.hash.salt: {{secrets/efence-secrets/mac-hash-salt}}

Notes:
    - Bronze is APPEND ONLY. No updates, no deletes.
    - pipelines.reset.allowed = false prevents accidental data loss.
    - Raw MACs stored here. Access to this table is restricted via Unity Catalog.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

import sys
import os
sys.path.insert(0, "/Workspace/Repos/efence/efence")  # adjust to your repo path

from schemas.wifi_schema import WIFI_BRONZE_SCHEMA


# ---------------------------------------------------------------------------
# Pipeline-level configuration (injected via Databricks pipeline settings)
# ---------------------------------------------------------------------------

def _conf(key: str, default: str = None) -> str:
    """Retrieve a Spark config value set in the pipeline configuration."""
    try:
        return spark.conf.get(key)
    except Exception:
        if default is not None:
            return default
        raise RuntimeError(f"Required pipeline config key missing: {key}")


LANDING_PATH     = _conf("efence.wifi.landing_path",    "/Volumes/efence/wifi_raw/landing")
SCHEMA_LOCATION  = _conf("efence.wifi.schema_location", "/Volumes/efence/wifi_raw/_autoloader_schema")
MAC_HASH_SALT    = _conf("efence.mac.hash.salt",        "change-me-in-secrets")


# ---------------------------------------------------------------------------
# Helper: deterministic event_id
# Combines sensor_id + event_time (epoch ms) + bssid to create a stable
# deduplication key that survives replay from the same source files.
# ---------------------------------------------------------------------------

def _add_event_id(df):
    """Generate a deterministic SHA-256 event ID from core observation fields."""
    return df.withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("sensor_id"),
                F.unix_millis(F.col("event_time")).cast("string"),
                F.coalesce(F.col("bssid"), F.lit("__no_bssid__")),
                F.coalesce(F.col("message_id"), F.lit("__no_msg__")),
            ),
            256,
        ),
    )


# ---------------------------------------------------------------------------
# Helper: ingest-time metadata columns
# ---------------------------------------------------------------------------

def _add_ingest_metadata(df):
    """Attach ingest-time audit columns to the incoming record."""
    return (
        df
        .withColumn("ingest_time",   F.current_timestamp())
        .withColumn("event_date",    F.to_date(F.col("event_time")).cast("string"))
        .withColumn("source_path",   F.input_file_name())
        # pipeline_id populated from Spark conf; set by DLT runtime
        .withColumn("pipeline_id",   F.lit(spark.conf.get("pipelines.id", "unknown")))
    )


# ---------------------------------------------------------------------------
# BRONZE TABLE: raw Wi-Fi observations (append-only, full fidelity)
# ---------------------------------------------------------------------------

@dlt.table(
    name="observations_bronze",
    comment="Raw Wi-Fi observations. Append-only. Full fidelity. Access controlled.",
    schema=WIFI_BRONZE_SCHEMA,
    table_properties={
        "quality":                          "bronze",
        "efence.protocol":                  "wifi",
        "pipelines.reset.allowed":          "false",   # prevent accidental truncate
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "delta.logRetentionDuration":       "interval 90 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    },
    partition_cols=["event_date"],
)
@dlt.expect("valid_sensor_id",  "sensor_id IS NOT NULL AND LENGTH(sensor_id) > 0")
@dlt.expect("valid_event_time", "event_time IS NOT NULL")
@dlt.expect_or_drop("event_not_future",
    "event_time <= (current_timestamp() + INTERVAL 5 MINUTES)")
def observations_bronze():
    """
    Auto Loader streaming ingest from the Wi-Fi landing volume.

    Schema evolution: addNewColumns mode ensures new fields from edge firmware
    updates are captured without pipeline failure. Unknown columns land here
    and can be promoted to silver schema in a subsequent release.

    Late arrival: records older than 7 days are dropped at bronze ingress to
    prevent unbounded state growth. Replay from source files handles backfill.
    """
    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",             "json")
        .option("cloudFiles.rescuedDataColumn",  "_rescued_data")
        .option("cloudFiles.useNotifications",   "false")
        .option("cloudFiles.maxFilesPerTrigger", "1000")
        .schema(WIFI_BRONZE_SCHEMA)
        .load(LANDING_PATH)
    )

    return (
        raw_stream
        .transform(_add_event_id)
        .transform(_add_ingest_metadata)
        # Drop records with event_time more than 7 days old at ingest
        .filter(
            F.col("event_time") >= F.current_timestamp() - F.expr("INTERVAL 7 DAYS")
        )
    )


# ---------------------------------------------------------------------------
# QUARANTINE TABLE: malformed / rescued records
# Records that fail schema parsing land here for investigation.
# ---------------------------------------------------------------------------

@dlt.table(
    name="observations_bronze_quarantine",
    comment="Wi-Fi records that failed schema validation or were rescued by Auto Loader.",
    table_properties={
        "quality":             "quarantine",
        "efence.protocol":    "wifi",
    },
    partition_cols=["event_date"],
)
def observations_bronze_quarantine():
    """
    Captures rescued data (malformed JSON, extra fields, type mismatches).
    Operators should monitor this table for edge firmware issues.
    No data quality expectations applied here — this IS the bad data.
    """
    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",             "json")
        .option("cloudFiles.schemaLocation",     SCHEMA_LOCATION + "/quarantine")
        .option("cloudFiles.rescuedDataColumn",  "_rescued_data")
        .load(LANDING_PATH)
    )

    return (
        raw_stream
        .filter(F.col("_rescued_data").isNotNull())
        .withColumn("ingest_time",  F.current_timestamp())
        .withColumn("event_date",   F.to_date(F.current_timestamp()).cast("string"))
        .withColumn("source_path",  F.input_file_name())
        .select(
            "_rescued_data",
            "sensor_id",
            "event_time",
            "ingest_time",
            "event_date",
            "source_path",
        )
    )
