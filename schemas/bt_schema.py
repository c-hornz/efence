"""
E-Fence Bluetooth Schema Definitions
Shared schema definitions for Bluetooth/BLE bronze and silver layers.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    BooleanType, TimestampType, MapType, ArrayType
)

# ---------------------------------------------------------------------------
# BRONZE: raw JSON landing schema
# ---------------------------------------------------------------------------

BT_BRONZE_SCHEMA = StructType([
    # Core observation identity
    StructField("sensor_id",         StringType(),    nullable=False),
    StructField("event_time",        TimestampType(), nullable=False),
    StructField("message_id",        StringType(),    nullable=True),
    StructField("capture_file",      StringType(),    nullable=True),

    # Device identity (raw, pre-hash)
    StructField("device_address",    StringType(),    nullable=True),   # BT/BLE MAC or IRK-derived
    StructField("address_randomized",BooleanType(),   nullable=True),   # LE random address flag
    StructField("device_name",       StringType(),    nullable=True),   # GAP device name if advertised
    StructField("advertising_uuid",  StringType(),    nullable=True),   # Complete 128-bit UUID

    # Manufacturer data
    StructField("manufacturer_id",   IntegerType(),   nullable=True),   # BT SIG company ID
    StructField("manufacturer_data", StringType(),    nullable=True),   # hex-encoded mfr data
    StructField("service_uuids",     ArrayType(StringType()), nullable=True),

    # Signal
    StructField("rssi_dbm",          IntegerType(),   nullable=True),
    StructField("tx_power",          IntegerType(),   nullable=True),   # dBm from adv packet

    # Protocol
    StructField("protocol_subtype",  StringType(),    nullable=True),   # BLE, BT_CLASSIC
    StructField("adv_type",          StringType(),    nullable=True),   # ADV_IND, ADV_NONCONN, etc.
    StructField("vendor_oui",        StringType(),    nullable=True),

    # Geospatial
    StructField("latitude",          DoubleType(),    nullable=True),
    StructField("longitude",         DoubleType(),    nullable=True),
    StructField("heading",           DoubleType(),    nullable=True),
    StructField("azimuth",           DoubleType(),    nullable=True),

    # Quality
    StructField("confidence",        DoubleType(),    nullable=True),

    # Raw
    StructField("raw_payload",       StringType(),    nullable=True),
    StructField("metadata",          MapType(StringType(), StringType()), nullable=True),
])


# ---------------------------------------------------------------------------
# SILVER: normalized and enriched schema
# ---------------------------------------------------------------------------

BT_SILVER_SCHEMA = StructType([
    # Deterministic record identity
    StructField("event_id",          StringType(),    nullable=False),  # SHA-256 dedup key
    StructField("sensor_id",         StringType(),    nullable=False),
    StructField("event_time",        TimestampType(), nullable=False),
    StructField("ingest_time",       TimestampType(), nullable=False),
    StructField("event_date",        StringType(),    nullable=False),  # partition key yyyy-MM-dd
    StructField("message_id",        StringType(),    nullable=True),
    StructField("capture_file",      StringType(),    nullable=True),

    # Device identity (hashed)
    StructField("device_address_hash", StringType(), nullable=True),   # SHA-256(address + salt)
    StructField("address_randomized",BooleanType(),   nullable=True),
    StructField("device_name",       StringType(),    nullable=True),   # retain (not typically PII)
    StructField("advertising_uuid",  StringType(),    nullable=True),

    # Manufacturer
    StructField("manufacturer_id",   IntegerType(),   nullable=True),
    StructField("manufacturer_name", StringType(),    nullable=True),   # resolved from BT SIG registry
    StructField("manufacturer_data", StringType(),    nullable=True),
    StructField("service_uuids",     ArrayType(StringType()), nullable=True),

    # Signal
    StructField("rssi_dbm",          IntegerType(),   nullable=True),
    StructField("tx_power",          IntegerType(),   nullable=True),
    StructField("path_loss_db",      IntegerType(),   nullable=True),   # tx_power - rssi if both present

    # Protocol
    StructField("protocol_subtype",  StringType(),    nullable=True),
    StructField("adv_type",          StringType(),    nullable=True),
    StructField("vendor_oui",        StringType(),    nullable=True),

    # Geospatial
    StructField("latitude",          DoubleType(),    nullable=True),
    StructField("longitude",         DoubleType(),    nullable=True),
    StructField("heading",           DoubleType(),    nullable=True),
    StructField("azimuth",           DoubleType(),    nullable=True),

    # Derived
    StructField("distance_est_m",    DoubleType(),    nullable=True),
    StructField("is_randomized_address", BooleanType(), nullable=True),
    StructField("is_connectable",    BooleanType(),   nullable=True),

    # Quality
    StructField("confidence",        DoubleType(),    nullable=True),
    StructField("dq_flags",          ArrayType(StringType()), nullable=True),

    # Lineage
    StructField("pipeline_id",       StringType(),    nullable=True),
    StructField("source_path",       StringType(),    nullable=True),
])
