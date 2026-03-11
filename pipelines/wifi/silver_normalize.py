"""
E-Fence Wi-Fi Silver Normalization Pipeline
============================================
Lakeflow Spark Declarative Pipeline (DLT)

Purpose:
    Read from Wi-Fi bronze, normalize, hash MAC addresses (PII),
    deduplicate within a watermark window, enrich, and apply
    rigorous data quality expectations.

Source table:  efence.wifi_raw.observations_bronze
Target table:  efence.wifi_silver.observations

Pipeline settings:
    catalog:  efence
    target:   wifi_silver
    configuration:
        efence.mac.hash.salt: {{secrets/efence-secrets/mac-hash-salt}}
        efence.wifi.dedup_watermark_hours: 2
        efence.wifi.late_arrival_drop_days: 7

Notes:
    - MAC addresses are hashed here. Bronze retains raw values under ACL.
    - RSSI-to-distance estimation uses free-space path loss model (A=40 dBm at 1m).
    - dropDuplicatesWithinWatermark requires Databricks Runtime 14.3+ (Spark 3.5+).
    - Silver is the authoritative normalized record for all downstream consumers.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, BooleanType

import sys
sys.path.insert(0, "/Workspace/Repos/efence/efence")

from schemas.wifi_schema import WIFI_SILVER_SCHEMA


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


MAC_HASH_SALT           = _conf("efence.mac.hash.salt", "change-me-in-secrets")
DEDUP_WATERMARK_HOURS   = int(_conf("efence.wifi.dedup_watermark_hours", "2"))
LATE_DROP_DAYS          = int(_conf("efence.wifi.late_arrival_drop_days", "7"))

# Free-space path loss reference: A = RSSI at 1 metre (dBm), N = path loss exponent
FSPL_REF_RSSI_DBM = -40   # typical 2.4 GHz at 1m
FSPL_EXPONENT     = 2.0   # free space; use 3.0 for indoor obstructed


# ---------------------------------------------------------------------------
# PII Hashing helpers
# ---------------------------------------------------------------------------

def _hash_mac(col_name: str, output_name: str, salt: str):
    """Return a Column expression that SHA-256 hashes a MAC address with a salt."""
    return F.sha2(
        F.concat(F.lower(F.regexp_replace(F.col(col_name), "[:\\-.]", "")), F.lit(salt)),
        256,
    ).alias(output_name)


def _is_randomized_mac(col_name: str) -> F.Column:
    """
    Heuristic: BLE random addresses set bit 6 of the most significant byte.
    For Wi-Fi, locally administered bit (bit 1 of MSB) indicates randomization.
    Pattern: second hex digit of first octet is 2, 6, A, or E.
    """
    return (
        F.regexp_extract(F.col(col_name), r"^([0-9a-fA-F]{2})", 1)
        .substr(2, 1)
        .isin(["2", "6", "a", "A", "e", "E"])
    )


def _estimate_distance(rssi_col: str) -> F.Column:
    """
    Estimate distance in metres from RSSI using log-distance path loss.
    d = 10 ^ ((A - RSSI) / (10 * N))
    where A = reference RSSI at 1m, N = path loss exponent.
    Returns NULL if RSSI is NULL or outside plausible range [-100, -20].
    """
    rssi = F.col(rssi_col).cast(DoubleType())
    in_range = (rssi >= -100) & (rssi <= -20)
    exponent = (F.lit(FSPL_REF_RSSI_DBM) - rssi) / F.lit(10.0 * FSPL_EXPONENT)
    return F.when(rssi.isNotNull() & in_range, F.pow(F.lit(10.0), exponent)).otherwise(F.lit(None).cast(DoubleType()))


# ---------------------------------------------------------------------------
# Band normalization
# ---------------------------------------------------------------------------

def _normalize_band(freq_col: str, band_col: str) -> F.Column:
    """
    Standardize band to canonical string: '2.4GHz', '5GHz', '6GHz'.
    Derives from frequency_mhz if band field is missing/malformed.
    """
    freq = F.col(freq_col)
    band_raw = F.lower(F.trim(F.col(band_col)))
    derived = (
        F.when(freq.between(2400, 2500), F.lit("2.4GHz"))
        .when(freq.between(5170, 5835), F.lit("5GHz"))
        .when(freq.between(5925, 7125), F.lit("6GHz"))
        .otherwise(F.lit(None).cast("string"))
    )
    canonical_band = (
        F.when(band_raw.isin("2.4", "2.4ghz", "2"), F.lit("2.4GHz"))
        .when(band_raw.isin("5", "5ghz"), F.lit("5GHz"))
        .when(band_raw.isin("6", "6ghz"), F.lit("6GHz"))
        .otherwise(derived)
    )
    return canonical_band


# ---------------------------------------------------------------------------
# Encryption normalization
# ---------------------------------------------------------------------------

def _normalize_encryption(enc_col: str) -> F.Column:
    """Canonicalize encryption strings to a controlled vocabulary."""
    raw = F.upper(F.trim(F.col(enc_col)))
    return (
        F.when(raw.isin("OPEN", "NONE", ""), F.lit("OPEN"))
        .when(raw.contains("WPA3"), F.lit("WPA3"))
        .when(raw.contains("WPA2"), F.lit("WPA2"))
        .when(raw.contains("WPA"),  F.lit("WPA"))
        .when(raw.contains("WEP"),  F.lit("WEP"))
        .otherwise(F.lit("UNKNOWN"))
    )


# ---------------------------------------------------------------------------
# DQ flag accumulator
# Builds an array of string flags for any soft quality issues.
# These are recorded but do NOT drop the row (hard drops are handled by
# @dlt.expect_or_drop on the table decorator).
# ---------------------------------------------------------------------------

def _build_dq_flags(df):
    """Attach a dq_flags array column recording soft data quality issues."""
    flags = F.array(
        F.when(F.col("rssi_dbm").isNull(),                   F.lit("MISSING_RSSI")).otherwise(F.lit(None)),
        F.when(F.col("bssid_hash").isNull(),                 F.lit("MISSING_BSSID")).otherwise(F.lit(None)),
        F.when(F.col("ssid").isNull(),                       F.lit("MISSING_SSID")).otherwise(F.lit(None)),
        F.when(F.col("channel").isNull(),                    F.lit("MISSING_CHANNEL")).otherwise(F.lit(None)),
        F.when(F.col("latitude").isNull(),                   F.lit("NO_GEOLOCATION")).otherwise(F.lit(None)),
        F.when((F.col("rssi_dbm") < -100) | (F.col("rssi_dbm") > 0),
                                                             F.lit("RSSI_OUT_OF_RANGE")).otherwise(F.lit(None)),
        F.when(F.col("confidence") < 0.5,                    F.lit("LOW_CONFIDENCE")).otherwise(F.lit(None)),
        F.when(F.col("is_randomized_mac"),                   F.lit("RANDOMIZED_MAC")).otherwise(F.lit(None)),
    )
    # Filter out nulls to keep the array clean
    return df.withColumn("dq_flags", F.array_compact(flags))


# ---------------------------------------------------------------------------
# Full normalization transform
# ---------------------------------------------------------------------------

def _normalize_wifi(df):
    """
    Apply all silver-layer normalizations to a Wi-Fi bronze DataFrame.
    Returns a DataFrame conforming to WIFI_SILVER_SCHEMA.
    """
    salt = MAC_HASH_SALT

    enriched = (
        df
        # ---- PII hashing ----
        .withColumn("bssid_hash",            _hash_mac("bssid",           "bssid_hash",            salt))
        .withColumn("client_mac_hash",       _hash_mac("client_mac",      "client_mac_hash",       salt))
        .withColumn("ap_mac_hash",           _hash_mac("ap_mac",          "ap_mac_hash",           salt))
        .withColumn("transmitter_mac_hash",  _hash_mac("transmitter_mac", "transmitter_mac_hash",  salt))

        # ---- Derived flags ----
        .withColumn("is_randomized_mac",
            F.when(F.col("bssid").isNotNull(), _is_randomized_mac("bssid")).otherwise(F.lit(None).cast(BooleanType())))
        .withColumn("is_hidden_ssid",
            (F.col("ssid").isNull()) | (F.length(F.col("ssid")) == 0))

        # ---- Band normalization ----
        .withColumn("band", _normalize_band("frequency_mhz", "band"))

        # ---- Encryption normalization ----
        .withColumn("encryption", _normalize_encryption("encryption"))

        # ---- Distance estimation ----
        .withColumn("distance_est_m", _estimate_distance("rssi_dbm"))

        # ---- Cast channel to int (defensive) ----
        .withColumn("channel", F.col("channel").cast("int"))

        # ---- Lineage ----
        .withColumn("pipeline_id", F.lit(spark.conf.get("pipelines.id", "unknown")))
    )

    enriched = _build_dq_flags(enriched)

    # Project to silver schema columns only
    return enriched.select([f.name for f in WIFI_SILVER_SCHEMA.fields])


# ---------------------------------------------------------------------------
# SILVER TABLE: normalized Wi-Fi observations
# ---------------------------------------------------------------------------

@dlt.table(
    name="observations",
    comment="Normalized, deduplicated Wi-Fi observations. MACs hashed. Silver quality.",
    schema=WIFI_SILVER_SCHEMA,
    table_properties={
        "quality":                          "silver",
        "efence.protocol":                  "wifi",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "delta.logRetentionDuration":       "interval 365 days",
    },
    partition_cols=["event_date"],
    # Liquid clustering alternative (use one or the other, not both):
    # cluster_by=["sensor_id", "event_date"],
)
@dlt.expect_or_drop("valid_event_id",   "event_id IS NOT NULL AND LENGTH(event_id) = 64")
@dlt.expect_or_drop("valid_sensor_id",  "sensor_id IS NOT NULL AND LENGTH(sensor_id) > 0")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dlt.expect_or_drop("event_not_ancient",
    f"event_time >= current_timestamp() - INTERVAL {LATE_DROP_DAYS} DAYS")
@dlt.expect("rssi_plausible",
    "rssi_dbm IS NULL OR (rssi_dbm >= -100 AND rssi_dbm <= 0)")
@dlt.expect("confidence_valid",
    "confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0)")
@dlt.expect("coordinates_paired",
    "(latitude IS NULL AND longitude IS NULL) OR (latitude IS NOT NULL AND longitude IS NOT NULL)")
@dlt.expect("valid_latitude",
    "latitude IS NULL OR (latitude BETWEEN -90 AND 90)")
@dlt.expect("valid_longitude",
    "longitude IS NULL OR (longitude BETWEEN -180 AND 180)")
def observations():
    """
    Read from bronze, apply watermark for late-arrival handling, deduplicate
    by event_id within the watermark window, normalize, and hash PII.

    Watermark of 2 hours means records arriving up to 2 hours late are still
    processed. Records older than the watermark are silently dropped by Spark
    structured streaming state management.

    dropDuplicatesWithinWatermark is the correct DLT streaming dedup mechanism
    as of Databricks Runtime 14.3+ (Spark 3.5+). For earlier runtimes, use
    dropDuplicates(["event_id"]) — note this has different state semantics.
    """
    return (
        dlt.read_stream("observations_bronze")
        .withWatermark("event_time", f"{DEDUP_WATERMARK_HOURS} hours")
        .dropDuplicatesWithinWatermark(["event_id"])
        .transform(_normalize_wifi)
    )
