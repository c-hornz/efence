"""
E-Fence Bluetooth Bronze Ingest Pipeline
==========================================
Lakeflow Spark Declarative Pipeline (DLT)

Purpose:
    Ingest raw Bluetooth/BLE observation JSON files from the Unity Catalog Volume
    landing zone using Auto Loader. Append only. Full fidelity preservation.

Target table:
    efence.bt_raw.observations_bronze

Pipeline settings (set in pipeline JSON/YAML config):
    catalog:  efence
    target:   bt_raw
    configuration:
        efence.bt.landing_path:    /Volumes/efence/bt_raw/landing
        efence.bt.schema_location: /Volumes/efence/bt_raw/_autoloader_schema
        efence.mac.hash.salt:      {{secrets/efence-secrets/mac-hash-salt}}

Design notes:
    - Logically separated from Wi-Fi pipeline at every layer.
    - Raspberry Pi edge device posts JSON events here (via curl/python/mqtt bridge).
    - BLE advertisements arrive at high frequency; maxFilesPerTrigger
      and maxBytesPerTrigger bound micro-batch size.
"""

import dlt
from pyspark.sql import functions as F

import sys
sys.path.insert(0, "/Workspace/Repos/efence/efence")

from schemas.bt_schema import BT_BRONZE_SCHEMA


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _conf(key: str, default: str = None) -> str:
    try:
        return spark.conf.get(key)
    except Exception:
        if default is not None:
            return default
        raise RuntimeError(f"Required pipeline config key missing: {key}")


LANDING_PATH    = _conf("efence.bt.landing_path",    "/Volumes/efence/bt_raw/landing")
SCHEMA_LOCATION = _conf("efence.bt.schema_location", "/Volumes/efence/bt_raw/_autoloader_schema")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _add_event_id(df):
    """
    Deterministic SHA-256 event ID for Bluetooth observations.
    Key components: sensor_id + event_time (epoch ms) + device_address + adv_type.
    adv_type is included because the same device may send multiple adv types
    in the same millisecond (e.g., ADV_IND + SCAN_RSP pair).
    """
    return df.withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("sensor_id"),
                F.unix_millis(F.col("event_time")).cast("string"),
                F.coalesce(F.col("device_address"), F.lit("__no_addr__")),
                F.coalesce(F.col("adv_type"),        F.lit("__no_adv_type__")),
                F.coalesce(F.col("message_id"),      F.lit("__no_msg__")),
            ),
            256,
        ),
    )


def _add_ingest_metadata(df):
    return (
        df
        .withColumn("ingest_time",  F.current_timestamp())
        .withColumn("event_date",   F.to_date(F.col("event_time")).cast("string"))
        .withColumn("source_path",  F.input_file_name())
        .withColumn("pipeline_id",  F.lit(spark.conf.get("pipelines.id", "unknown")))
    )


# ---------------------------------------------------------------------------
# BRONZE TABLE: raw Bluetooth observations
# ---------------------------------------------------------------------------

@dlt.table(
    name="observations_bronze",
    comment="Raw Bluetooth/BLE observations. Append-only. Full fidelity. Access controlled.",
    schema=BT_BRONZE_SCHEMA,
    table_properties={
        "quality":                          "bronze",
        "efence.protocol":                  "bluetooth",
        "pipelines.reset.allowed":          "false",
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
    Auto Loader streaming ingest from the Bluetooth landing volume.

    BLE devices advertise at intervals of 20ms–10s. A single sensor may
    generate 10,000+ events per minute in high-density environments.
    maxFilesPerTrigger and micro-batch interval control throughput.

    service_uuids is an array type — Auto Loader infers this from JSON arrays.
    metadata is a map type for arbitrary key-value extensions from firmware.
    """
    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",             "json")
        .option("cloudFiles.rescuedDataColumn",  "_rescued_data")
        .option("cloudFiles.maxFilesPerTrigger", "2000")
        .option("cloudFiles.maxBytesPerTrigger", "512m")
        .schema(BT_BRONZE_SCHEMA)
        .load(LANDING_PATH)
    )

    return (
        raw_stream
        .transform(_add_event_id)
        .transform(_add_ingest_metadata)
        .filter(
            F.col("event_time") >= F.current_timestamp() - F.expr("INTERVAL 7 DAYS")
        )
    )


# ---------------------------------------------------------------------------
# QUARANTINE TABLE: malformed BT records
# ---------------------------------------------------------------------------

@dlt.table(
    name="observations_bronze_quarantine",
    comment="Bluetooth records that failed parsing or were rescued by Auto Loader.",
    table_properties={
        "quality":          "quarantine",
        "efence.protocol":  "bluetooth",
    },
    partition_cols=["event_date"],
)
def observations_bronze_quarantine():
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
