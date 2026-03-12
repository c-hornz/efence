"""
E-Fence Bluetooth Silver Normalization Pipeline
================================================
Lakeflow Spark Declarative Pipeline (DLT)

Purpose:
    Read from Bluetooth bronze, normalize, hash device addresses (PII),
    deduplicate within watermark window, derive path loss and distance,
    resolve manufacturer IDs, and apply rigorous data quality expectations.

Source table:  efence.bt_raw.observations_bronze
Target table:  efence.bt_silver.observations

Pipeline settings:
    catalog:  efence
    target:   bt_silver
    configuration:
        efence.mac.hash.salt: {{secrets/efence-secrets/mac-hash-salt}}
        efence.bt.dedup_watermark_hours: 1
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, BooleanType, IntegerType

import sys
sys.path.insert(0, "/Workspace/Repos/efence/efence")

from schemas.bt_schema import BT_SILVER_SCHEMA


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


MAC_HASH_SALT         = _conf("efence.mac.hash.salt", "change-me-in-secrets")
DEDUP_WATERMARK_HOURS = int(_conf("efence.bt.dedup_watermark_hours", "1"))
LATE_DROP_DAYS        = int(_conf("efence.bt.late_arrival_drop_days", "7"))

# BLE path loss model (log-distance): d = 10 ^ ((TxPower - RSSI) / (10 * N))
BLE_PATH_LOSS_EXPONENT = 2.0   # free space; 2.5-3.5 for indoor


# ---------------------------------------------------------------------------
# PII hashing
# ---------------------------------------------------------------------------

def _hash_address(col_name: str, output_name: str, salt: str) -> F.Column:
    """SHA-256 hash of a device address (BT MAC or BLE IRK-derived) with salt."""
    return F.sha2(
        F.concat(
            F.lower(F.regexp_replace(F.col(col_name), "[:\\-. ]", "")),
            F.lit(salt),
        ),
        256,
    ).alias(output_name)


# ---------------------------------------------------------------------------
# BLE random address detection
# ---------------------------------------------------------------------------

def _is_ble_random_address(col_name: str) -> F.Column:
    """
    BLE random address: top 2 bits of first octet determine type:
      11 = Static random
      01 = Resolvable private
      00 = Non-resolvable private
    Heuristic: if the address_randomized flag is set in the payload, use it.
    Otherwise fall back to the bit pattern (bits 7-6 of first byte).
    The second hex char of first octet: C/D/E/F = static random,
    4/5/6/7 = resolvable private, 0/1/2/3 = non-resolvable private.
    All of these are randomized. Public addresses never set these bits.
    """
    first_octet_hi = (
        F.regexp_extract(F.col(col_name), r"^([0-9a-fA-F]{2})", 1)
        .substr(2, 1)
        .isin(["c", "C", "d", "D", "e", "E", "f", "F",
               "4", "5", "6", "7",
               "0", "1", "2", "3"])
    )
    return F.coalesce(F.col("address_randomized"), first_octet_hi)


# ---------------------------------------------------------------------------
# Path loss and distance estimation
# ---------------------------------------------------------------------------

def _compute_path_loss(df):
    """
    Compute path loss when both tx_power and rssi_dbm are available.
    path_loss_db = tx_power - rssi_dbm
    """
    return df.withColumn(
        "path_loss_db",
        F.when(
            F.col("tx_power").isNotNull() & F.col("rssi_dbm").isNotNull(),
            (F.col("tx_power") - F.col("rssi_dbm")).cast(IntegerType()),
        ).otherwise(F.lit(None).cast(IntegerType())),
    )


def _estimate_distance(df):
    """
    Estimate distance in metres from BLE RSSI using log-distance path loss.
    Uses tx_power if available (more accurate), falls back to reference of -59 dBm at 1m.
    d = 10 ^ ((TxPower - RSSI) / (10 * N))
    """
    ref_tx = F.coalesce(F.col("tx_power"), F.lit(-59)).cast(DoubleType())
    rssi   = F.col("rssi_dbm").cast(DoubleType())
    in_range = rssi.between(-110, 0) & ref_tx.between(-100, 20)
    exponent = (ref_tx - rssi) / F.lit(10.0 * BLE_PATH_LOSS_EXPONENT)
    dist = F.when(
        rssi.isNotNull() & in_range,
        F.round(F.pow(F.lit(10.0), exponent), 2),
    ).otherwise(F.lit(None).cast(DoubleType()))
    return df.withColumn("distance_est_m", dist)


# ---------------------------------------------------------------------------
# Protocol subtype normalization
# ---------------------------------------------------------------------------

def _normalize_protocol(df):
    raw = F.upper(F.trim(F.col("protocol_subtype")))
    normalized = (
        F.when(raw.isin("BLE", "BTLE", "BT_LE", "LOW_ENERGY"), F.lit("BLE"))
        .when(raw.isin("CLASSIC", "BT_CLASSIC", "BR/EDR", "BREDR"),  F.lit("BT_CLASSIC"))
        .otherwise(F.lit("UNKNOWN"))
    )
    return df.withColumn("protocol_subtype", normalized)


def _normalize_adv_type(df):
    """Canonicalize BLE advertising type names."""
    raw = F.upper(F.trim(F.col("adv_type")))
    normalized = (
        F.when(raw.isin("ADV_IND", "CONNECTABLE_UNDIRECTED"),        F.lit("ADV_IND"))
        .when(raw.isin("ADV_DIRECT_IND", "CONNECTABLE_DIRECTED"),    F.lit("ADV_DIRECT_IND"))
        .when(raw.isin("ADV_NONCONN_IND", "NON_CONNECTABLE"),        F.lit("ADV_NONCONN_IND"))
        .when(raw.isin("SCAN_RSP", "SCAN_RESPONSE"),                 F.lit("SCAN_RSP"))
        .when(raw.isin("ADV_SCAN_IND", "SCANNABLE_UNDIRECTED"),      F.lit("ADV_SCAN_IND"))
        .otherwise(raw)
    )
    return df.withColumn("adv_type", normalized)


# ---------------------------------------------------------------------------
# Connectable flag
# ---------------------------------------------------------------------------

def _add_connectable_flag(df):
    return df.withColumn(
        "is_connectable",
        F.col("adv_type").isin("ADV_IND", "ADV_DIRECT_IND"),
    )


# ---------------------------------------------------------------------------
# DQ flags
# ---------------------------------------------------------------------------

def _build_dq_flags(df):
    flags = F.array(
        F.when(F.col("rssi_dbm").isNull(),              F.lit("MISSING_RSSI")).otherwise(F.lit(None)),
        F.when(F.col("device_address_hash").isNull(),   F.lit("MISSING_DEVICE_ADDRESS")).otherwise(F.lit(None)),
        F.when(F.col("protocol_subtype") == "UNKNOWN",  F.lit("UNKNOWN_PROTOCOL")).otherwise(F.lit(None)),
        F.when(F.col("latitude").isNull(),              F.lit("NO_GEOLOCATION")).otherwise(F.lit(None)),
        F.when(F.col("is_randomized_address"),          F.lit("RANDOMIZED_ADDRESS")).otherwise(F.lit(None)),
        F.when((F.col("rssi_dbm") < -110) | (F.col("rssi_dbm") > 20),
                                                        F.lit("RSSI_OUT_OF_RANGE")).otherwise(F.lit(None)),
        F.when(F.col("confidence") < 0.5,              F.lit("LOW_CONFIDENCE")).otherwise(F.lit(None)),
    )
    return df.withColumn("dq_flags", F.array_compact(flags))


# ---------------------------------------------------------------------------
# Full normalization transform
# ---------------------------------------------------------------------------

def _normalize_bt(df):
    """
    Apply all silver-layer normalizations to a Bluetooth bronze DataFrame.
    Returns a DataFrame conforming to BT_SILVER_SCHEMA.
    """
    salt = MAC_HASH_SALT

    enriched = (
        df
        # ---- PII hashing ----
        .withColumn("device_address_hash",
            _hash_address("device_address", "device_address_hash", salt))

        # ---- Random address detection ----
        .withColumn("is_randomized_address",
            F.when(F.col("device_address").isNotNull(),
                _is_ble_random_address("device_address"))
            .otherwise(F.lit(None).cast(BooleanType())))

        # ---- Protocol normalization ----
        .transform(_normalize_protocol)
        .transform(_normalize_adv_type)
        .transform(_add_connectable_flag)

        # ---- Manufacturer name placeholder (future: BT SIG lookup) ----
        .withColumn("manufacturer_name", F.lit(None).cast("string"))

        # ---- Signal enrichment ----
        .transform(_compute_path_loss)
        .transform(_estimate_distance)

        # ---- Lineage ----
        .withColumn("pipeline_id", F.lit(spark.conf.get("pipelines.id", "unknown")))
    )

    enriched = _build_dq_flags(enriched)

    return enriched.select([f.name for f in BT_SILVER_SCHEMA.fields])


# ---------------------------------------------------------------------------
# SILVER TABLE: normalized Bluetooth observations
# ---------------------------------------------------------------------------

@dlt.table(
    name="observations",
    comment="Normalized, deduplicated Bluetooth/BLE observations. Addresses hashed. Silver quality.",
    schema=BT_SILVER_SCHEMA,
    table_properties={
        "quality":                          "silver",
        "efence.protocol":                  "bluetooth",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "delta.logRetentionDuration":       "interval 365 days",
    },
    partition_cols=["event_date"],
)
@dlt.expect_or_drop("valid_event_id",   "event_id IS NOT NULL AND LENGTH(event_id) = 64")
@dlt.expect_or_drop("valid_sensor_id",  "sensor_id IS NOT NULL AND LENGTH(sensor_id) > 0")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dlt.expect_or_drop("event_not_ancient",
    f"event_time >= current_timestamp() - INTERVAL {LATE_DROP_DAYS} DAYS")
@dlt.expect("rssi_plausible",
    "rssi_dbm IS NULL OR (rssi_dbm >= -110 AND rssi_dbm <= 20)")
@dlt.expect("confidence_valid",
    "confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0)")
@dlt.expect("coordinates_paired",
    "(latitude IS NULL AND longitude IS NULL) OR (latitude IS NOT NULL AND longitude IS NOT NULL)")
@dlt.expect("valid_protocol_subtype",
    "protocol_subtype IN ('BLE', 'BT_CLASSIC', 'UNKNOWN')")
def observations():
    """
    Read from BT bronze, watermark, deduplicate by event_id, normalize, and hash PII.

    BLE dedup watermark is 1 hour (shorter than Wi-Fi because BLE advertisements
    are high-frequency and same-device duplicates appear within seconds).
    A 1-hour window balances late-arrival tolerance vs. state store memory.
    """
    return (
        dlt.read_stream("observations_bronze")
        .withWatermark("event_time", f"{DEDUP_WATERMARK_HOURS} hours")
        .dropDuplicatesWithinWatermark(["event_id"])
        .transform(_normalize_bt)
    )
