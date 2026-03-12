"""
E-Fence Gold Correlation & Analytics Pipeline
==============================================
Lakeflow Spark Declarative Pipeline (DLT)

Purpose:
    Correlate Wi-Fi and Bluetooth silver observations to produce:
    1. Unified device timeline (one row per device observation, any protocol)
    2. Co-presence events (same location, both Wi-Fi and BT seen together)
    3. Device sighting windows (first/last seen per device per day)
    4. Perimeter intrusion scoring (unknown device with high confidence + proximity)
    5. Repeated visitor analysis

Source tables:
    efence.wifi_silver.observations       (external Delta table, read as stream)
    efence.bt_silver.observations         (external Delta table, read as stream)

Target tables (all in efence.gold):
    device_timeline
    copresence_events
    device_sighting_windows
    perimeter_alerts

Pipeline settings:
    catalog:  efence
    target:   gold
    configuration:
        efence.gold.copresence_window_seconds: 30
        efence.gold.proximity_threshold_m:     50.0
        efence.gold.alert_rssi_threshold_dbm: -65
        efence.gold.alert_min_confidence:      0.7

Notes:
    - Reads from silver tables in OTHER schemas using spark.readStream.table().
      This is correct for reading external (non-DLT-managed) Delta tables in a
      DLT pipeline. Do NOT use dlt.read() across pipeline boundaries.
    - Gold tables are append-only for the timeline; sighting_windows uses
      APPLY CHANGES INTO (SCD Type 1) for latest state per device.
    - Only hashed identifiers appear in gold — no raw MACs.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, BooleanType, IntegerType, ArrayType
)


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


CATALOG                 = _conf("efence.gold.catalog", spark.conf.get("pipelines.catalog", "efence"))
COPRESENCE_WINDOW_SECS  = int(_conf("efence.gold.copresence_window_seconds", "30"))
PROXIMITY_THRESHOLD_M   = float(_conf("efence.gold.proximity_threshold_m",  "50.0"))
ALERT_RSSI_THRESHOLD    = int(_conf("efence.gold.alert_rssi_threshold_dbm", "-65"))
ALERT_MIN_CONFIDENCE    = float(_conf("efence.gold.alert_min_confidence",   "0.7"))


# ---------------------------------------------------------------------------
# Source readers
# Read silver tables as Delta streaming sources (not DLT-managed tables).
# spark.readStream.table() is the correct cross-pipeline streaming read pattern.
# ---------------------------------------------------------------------------

def _read_wifi_silver():
    return (
        spark.readStream
        .option("readChangeFeed", "false")   # append-only silver; no CDF needed
        .table(f"{CATALOG}.wifi_silver.observations")
        .select(
            "event_id",
            "sensor_id",
            "event_time",
            "ingest_time",
            "event_date",
            "bssid_hash",
            "ssid",
            "rssi_dbm",
            "band",
            "encryption",
            "channel",
            "distance_est_m",
            "latitude",
            "longitude",
            "confidence",
            "is_randomized_mac",
            "dq_flags",
            F.lit("WIFI").alias("protocol"),
            F.col("bssid_hash").alias("device_id_hash"),   # unified device key
        )
    )


def _read_bt_silver():
    return (
        spark.readStream
        .option("readChangeFeed", "false")
        .table(f"{CATALOG}.bt_silver.observations")
        .select(
            "event_id",
            "sensor_id",
            "event_time",
            "ingest_time",
            "event_date",
            "device_address_hash",
            "device_name",
            "rssi_dbm",
            "protocol_subtype",
            "adv_type",
            "manufacturer_id",
            "distance_est_m",
            "latitude",
            "longitude",
            "confidence",
            "is_randomized_address",
            "dq_flags",
            F.lit("BLUETOOTH").alias("protocol"),
            F.col("device_address_hash").alias("device_id_hash"),   # unified device key
        )
    )


# ---------------------------------------------------------------------------
# TABLE 1: Unified Device Timeline
# Append-only union of Wi-Fi and BT observations with protocol tag.
# Used for full replay, correlation, and downstream ML feature generation.
# ---------------------------------------------------------------------------

DEVICE_TIMELINE_SCHEMA = StructType([
    StructField("event_id",          StringType(),    nullable=False),
    StructField("sensor_id",         StringType(),    nullable=False),
    StructField("event_time",        TimestampType(), nullable=False),
    StructField("ingest_time",       TimestampType(), nullable=False),
    StructField("event_date",        StringType(),    nullable=False),
    StructField("protocol",          StringType(),    nullable=False),   # WIFI | BLUETOOTH
    StructField("device_id_hash",    StringType(),    nullable=True),
    StructField("device_name",       StringType(),    nullable=True),    # BT only
    StructField("ssid",              StringType(),    nullable=True),    # WiFi only
    StructField("rssi_dbm",          IntegerType(),   nullable=True),
    StructField("distance_est_m",    DoubleType(),    nullable=True),
    StructField("latitude",          DoubleType(),    nullable=True),
    StructField("longitude",         DoubleType(),    nullable=True),
    StructField("confidence",        DoubleType(),    nullable=True),
    StructField("protocol_detail",   StringType(),    nullable=True),    # band/encryption for WiFi, subtype for BT
    StructField("dq_flags",          ArrayType(StringType()), nullable=True),
])


@dlt.table(
    name="device_timeline",
    comment="Unified append-only timeline of all RF device observations across both protocols.",
    schema=DEVICE_TIMELINE_SCHEMA,
    table_properties={
        "quality":                          "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "delta.logRetentionDuration":       "interval 365 days",
    },
    partition_cols=["event_date", "protocol"],
)
@dlt.expect_or_drop("valid_event_id",   "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_protocol",   "protocol IN ('WIFI', 'BLUETOOTH')")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
def device_timeline():
    """
    Union of normalized Wi-Fi and Bluetooth silver observations.
    Provides the base for all cross-protocol correlation.

    Watermark of 2 hours handles both protocols' maximum late-arrival windows.
    UNION ALL is the correct approach here — we do NOT deduplicate across
    protocols because the same physical device may appear on both.
    """
    wifi = _read_wifi_silver().select(
        "event_id", "sensor_id", "event_time", "ingest_time", "event_date",
        "protocol", "device_id_hash",
        F.lit(None).cast("string").alias("device_name"),
        "ssid",
        "rssi_dbm", "distance_est_m", "latitude", "longitude", "confidence",
        F.concat_ws("/", F.col("band"), F.col("encryption")).alias("protocol_detail"),
        "dq_flags",
    )

    bt = _read_bt_silver().select(
        "event_id", "sensor_id", "event_time", "ingest_time", "event_date",
        "protocol", "device_id_hash",
        "device_name",
        F.lit(None).cast("string").alias("ssid"),
        "rssi_dbm", "distance_est_m", "latitude", "longitude", "confidence",
        F.col("protocol_subtype").alias("protocol_detail"),
        "dq_flags",
    )

    return (
        wifi.unionByName(bt)
        .withWatermark("event_time", "2 hours")
    )


# ---------------------------------------------------------------------------
# TABLE 2: Co-presence Events
# Window join: Wi-Fi and BT observations from the same sensor within N seconds.
# Indicates a physical device likely holding both Wi-Fi and BT radios.
# ---------------------------------------------------------------------------

COPRESENCE_SCHEMA = StructType([
    StructField("copresence_id",     StringType(),    nullable=False),  # SHA-256 of both event_ids
    StructField("sensor_id",         StringType(),    nullable=False),
    StructField("window_start",      TimestampType(), nullable=False),
    StructField("window_end",        TimestampType(), nullable=False),
    StructField("event_date",        StringType(),    nullable=False),
    StructField("wifi_event_id",     StringType(),    nullable=False),
    StructField("bt_event_id",       StringType(),    nullable=False),
    StructField("wifi_device_hash",  StringType(),    nullable=True),
    StructField("bt_device_hash",    StringType(),    nullable=True),
    StructField("wifi_rssi_dbm",     IntegerType(),   nullable=True),
    StructField("bt_rssi_dbm",       IntegerType(),   nullable=True),
    StructField("wifi_distance_m",   DoubleType(),    nullable=True),
    StructField("bt_distance_m",     DoubleType(),    nullable=True),
    StructField("latitude",          DoubleType(),    nullable=True),
    StructField("longitude",         DoubleType(),    nullable=True),
    StructField("copresence_score",  DoubleType(),    nullable=True),   # 0.0–1.0
    StructField("ingest_time",       TimestampType(), nullable=False),
])


@dlt.table(
    name="copresence_events",
    comment="Wi-Fi and Bluetooth co-presence events: same sensor, overlapping time window.",
    schema=COPRESENCE_SCHEMA,
    table_properties={
        "quality":                          "gold",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    partition_cols=["event_date"],
)
@dlt.expect_or_drop("valid_copresence_id", "copresence_id IS NOT NULL")
def copresence_events():
    """
    Stream-stream window join: Wi-Fi and BT observations from the same sensor
    within a configurable time window (default 30 seconds).

    This is the primary cross-protocol entity resolution signal:
    - A device seen on both Wi-Fi (as a BSSID or client) and BT within 30 seconds
      at the same sensor is very likely the same physical device.
    - copresence_score combines RSSI proximity confidence and temporal overlap.

    Spark stream-stream joins require watermarks on BOTH sides.
    The join window MUST be <= the watermark interval.
    """
    wifi_stream = (
        _read_wifi_silver()
        .withWatermark("event_time", "2 hours")
        .select(
            F.col("event_id").alias("wifi_event_id"),
            F.col("sensor_id").alias("wifi_sensor_id"),
            F.col("event_time").alias("wifi_event_time"),
            F.col("device_id_hash").alias("wifi_device_hash"),
            F.col("rssi_dbm").alias("wifi_rssi_dbm"),
            F.col("distance_est_m").alias("wifi_distance_m"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("confidence").alias("wifi_confidence"),
        )
    )

    bt_stream = (
        _read_bt_silver()
        .withWatermark("event_time", "2 hours")
        .select(
            F.col("event_id").alias("bt_event_id"),
            F.col("sensor_id").alias("bt_sensor_id"),
            F.col("event_time").alias("bt_event_time"),
            F.col("device_id_hash").alias("bt_device_hash"),
            F.col("rssi_dbm").alias("bt_rssi_dbm"),
            F.col("distance_est_m").alias("bt_distance_m"),
            F.col("confidence").alias("bt_confidence"),
        )
    )

    window_secs = COPRESENCE_WINDOW_SECS

    joined = wifi_stream.join(
        bt_stream,
        on=(
            (wifi_stream["wifi_sensor_id"] == bt_stream["bt_sensor_id"])
            & (
                wifi_stream["wifi_event_time"].between(
                    bt_stream["bt_event_time"] - F.expr(f"INTERVAL {window_secs} SECONDS"),
                    bt_stream["bt_event_time"] + F.expr(f"INTERVAL {window_secs} SECONDS"),
                )
            )
        ),
        how="inner",
    )

    return (
        joined
        .withColumn(
            "copresence_id",
            F.sha2(F.concat_ws("|", F.col("wifi_event_id"), F.col("bt_event_id")), 256),
        )
        .withColumn("sensor_id",   F.col("wifi_sensor_id"))
        .withColumn("window_start",
            F.least(F.col("wifi_event_time"), F.col("bt_event_time")))
        .withColumn("window_end",
            F.greatest(F.col("wifi_event_time"), F.col("bt_event_time")))
        .withColumn("event_date",  F.to_date(F.col("window_start")).cast("string"))
        .withColumn(
            "copresence_score",
            # Score: average confidence of both observations, weighted 0.0–1.0
            F.round(
                (F.coalesce(F.col("wifi_confidence"), F.lit(0.5))
                 + F.coalesce(F.col("bt_confidence"), F.lit(0.5))) / 2.0,
                3,
            ),
        )
        .withColumn("latitude",    F.col("latitude"))
        .withColumn("longitude",   F.col("longitude"))
        .withColumn("ingest_time", F.current_timestamp())
        .select([f.name for f in COPRESENCE_SCHEMA.fields])
    )


# ---------------------------------------------------------------------------
# TABLE 3: Perimeter Alerts
# Devices with strong RSSI + high confidence + unknown/randomized identity.
# Designed for near-real-time threat detection.
# ---------------------------------------------------------------------------

PERIMETER_ALERT_SCHEMA = StructType([
    StructField("alert_id",          StringType(),    nullable=False),
    StructField("sensor_id",         StringType(),    nullable=False),
    StructField("event_time",        TimestampType(), nullable=False),
    StructField("event_date",        StringType(),    nullable=False),
    StructField("protocol",          StringType(),    nullable=False),
    StructField("device_id_hash",    StringType(),    nullable=True),
    StructField("rssi_dbm",          IntegerType(),   nullable=True),
    StructField("distance_est_m",    DoubleType(),    nullable=True),
    StructField("latitude",          DoubleType(),    nullable=True),
    StructField("longitude",         DoubleType(),    nullable=True),
    StructField("confidence",        DoubleType(),    nullable=True),
    StructField("alert_reasons",     ArrayType(StringType()), nullable=True),
    StructField("threat_score",      DoubleType(),    nullable=True),   # 0.0–1.0
    StructField("ingest_time",       TimestampType(), nullable=False),
])


@dlt.table(
    name="perimeter_alerts",
    comment="High-priority perimeter alerts: strong signal + high confidence + unknown/randomized device.",
    schema=PERIMETER_ALERT_SCHEMA,
    table_properties={
        "quality":                          "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.logRetentionDuration":       "interval 730 days",   # retain alerts longer
    },
    partition_cols=["event_date", "protocol"],
)
@dlt.expect_or_drop("valid_alert_id",   "alert_id IS NOT NULL")
@dlt.expect_or_drop("has_sensor",       "sensor_id IS NOT NULL")
def perimeter_alerts():
    """
    Scoring heuristic for perimeter intrusion detection:
    - Strong RSSI (device is close to the sensor)
    - High confidence observation
    - Randomized/unknown MAC (device is hiding its identity)
    - Unknown SSID (hidden network or probe request)

    threat_score = normalized composite of proximity, confidence, and anomaly signals.
    This is a rule-based baseline — replace with ML model output in production.

    Reads from the device_timeline DLT table (same pipeline, so use dlt.read_stream).
    """
    rssi_threshold  = ALERT_RSSI_THRESHOLD
    conf_threshold  = ALERT_MIN_CONFIDENCE

    timeline = (
        dlt.read_stream("device_timeline")
        .withWatermark("event_time", "1 hour")
    )

    # Build alert reason flags
    scored = (
        timeline
        .filter(
            (F.col("rssi_dbm") >= rssi_threshold)
            & (F.col("confidence") >= conf_threshold)
        )
        .withColumn(
            "alert_reasons",
            F.array_compact(F.array(
                F.when(F.col("rssi_dbm") >= -50, F.lit("VERY_CLOSE_PROXIMITY")).otherwise(F.lit(None)),
                F.when(F.col("rssi_dbm").between(-65, -51), F.lit("CLOSE_PROXIMITY")).otherwise(F.lit(None)),
                F.when(
                    F.array_contains(F.col("dq_flags"), "RANDOMIZED_MAC")
                    | F.array_contains(F.col("dq_flags"), "RANDOMIZED_ADDRESS"),
                    F.lit("RANDOMIZED_IDENTIFIER"),
                ).otherwise(F.lit(None)),
                F.when(
                    F.array_contains(F.col("dq_flags"), "MISSING_SSID"),
                    F.lit("HIDDEN_NETWORK"),
                ).otherwise(F.lit(None)),
                F.when(F.col("distance_est_m") < 10.0, F.lit("WITHIN_10M")).otherwise(F.lit(None)),
            )),
        )
        # Only generate alerts when at least one reason is present
        .filter(F.size(F.col("alert_reasons")) > 0)
        .withColumn(
            "threat_score",
            F.round(
                # RSSI component: normalized from [-65, -20] to [0.3, 1.0]
                F.least(F.lit(1.0),
                    F.lit(0.3) + (F.col("rssi_dbm") - F.lit(rssi_threshold)).cast(DoubleType())
                    / F.lit(45.0) * F.lit(0.7)
                )
                # Confidence component
                * F.coalesce(F.col("confidence"), F.lit(0.5))
                # Alert reason multiplier: more reasons = higher score
                * F.least(F.lit(1.0), F.lit(0.7) + F.size(F.col("alert_reasons")) * F.lit(0.1)),
                3,
            ),
        )
        .withColumn(
            "alert_id",
            F.sha2(F.concat_ws("|", F.col("event_id"), F.lit("ALERT")), 256),
        )
        .withColumn("ingest_time", F.current_timestamp())
    )

    return scored.select([f.name for f in PERIMETER_ALERT_SCHEMA.fields])
