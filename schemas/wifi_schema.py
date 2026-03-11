"""
E-Fence Wi-Fi Schema Definitions
Shared schema definitions for Wi-Fi bronze and silver layers.
Used by DLT pipeline code to enforce and validate structure.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    BooleanType, TimestampType, MapType, ArrayType
)

# ---------------------------------------------------------------------------
# BRONZE: raw JSON landing schema
# Used by Auto Loader to validate inbound events from edge sensors.
# All fields are nullable at bronze except sensor_id + event_time.
# Auto Loader will apply this schema during file ingestion.
# ---------------------------------------------------------------------------

WIFI_BRONZE_SCHEMA = StructType([
    # Core observation identity
    StructField("sensor_id",         StringType(),    nullable=False),
    StructField("event_time",        TimestampType(), nullable=False),
    StructField("message_id",        StringType(),    nullable=True),   # dedup key from edge
    StructField("capture_file",      StringType(),    nullable=True),   # pcap or capture reference

    # RF target fields
    StructField("bssid",             StringType(),    nullable=True),   # AP MAC (raw, pre-hash)
    StructField("ssid",              StringType(),    nullable=True),
    StructField("channel",           IntegerType(),   nullable=True),
    StructField("frequency_mhz",     IntegerType(),   nullable=True),
    StructField("band",              StringType(),    nullable=True),   # "2.4", "5", "6"

    # Signal quality
    StructField("rssi_dbm",          IntegerType(),   nullable=True),
    StructField("snr_db",            DoubleType(),    nullable=True),

    # Security / protocol
    StructField("encryption",        StringType(),    nullable=True),   # WPA2, WPA3, OPEN, etc.
    StructField("auth_mode",         StringType(),    nullable=True),   # PSK, EAP, SAE, etc.
    StructField("frame_type",        StringType(),    nullable=True),   # beacon, probe_req, etc.

    # Device identifiers (raw, will be hashed in silver)
    StructField("vendor_oui",        StringType(),    nullable=True),
    StructField("client_mac",        StringType(),    nullable=True),   # station MAC if observed
    StructField("ap_mac",            StringType(),    nullable=True),   # same as bssid or BSS
    StructField("transmitter_mac",   StringType(),    nullable=True),   # TA field from frame

    # Geospatial (null for fixed sensors without GPS)
    StructField("latitude",          DoubleType(),    nullable=True),
    StructField("longitude",         DoubleType(),    nullable=True),
    StructField("heading",           DoubleType(),    nullable=True),   # degrees 0-359
    StructField("azimuth",           DoubleType(),    nullable=True),   # directional bearing

    # Quality / confidence
    StructField("confidence",        DoubleType(),    nullable=True),   # 0.0–1.0

    # Raw / opaque
    StructField("raw_payload",       StringType(),    nullable=True),   # base64 encoded frame
    StructField("metadata",          MapType(StringType(), StringType()), nullable=True),
])


# ---------------------------------------------------------------------------
# SILVER: normalized and enriched schema
# MACs are hashed. Fields are cast and validated.
# event_id is a deterministic SHA-256 hash generated at bronze->silver.
# ---------------------------------------------------------------------------

WIFI_SILVER_SCHEMA = StructType([
    # Deterministic record identity
    StructField("event_id",          StringType(),    nullable=False),  # SHA-256 dedup key
    StructField("sensor_id",         StringType(),    nullable=False),
    StructField("event_time",        TimestampType(), nullable=False),
    StructField("ingest_time",       TimestampType(), nullable=False),
    StructField("event_date",        StringType(),    nullable=False),  # partition key yyyy-MM-dd
    StructField("message_id",        StringType(),    nullable=True),
    StructField("capture_file",      StringType(),    nullable=True),

    # RF target fields (validated / cast)
    StructField("bssid_hash",        StringType(),    nullable=True),   # SHA-256(bssid + salt)
    StructField("ssid",              StringType(),    nullable=True),
    StructField("channel",           IntegerType(),   nullable=True),
    StructField("frequency_mhz",     IntegerType(),   nullable=True),
    StructField("band",              StringType(),    nullable=True),

    # Signal quality
    StructField("rssi_dbm",          IntegerType(),   nullable=True),
    StructField("snr_db",            DoubleType(),    nullable=True),

    # Security / protocol
    StructField("encryption",        StringType(),    nullable=True),
    StructField("auth_mode",         StringType(),    nullable=True),
    StructField("frame_type",        StringType(),    nullable=True),

    # Device identifiers (hashed)
    StructField("vendor_oui",        StringType(),    nullable=True),
    StructField("client_mac_hash",   StringType(),    nullable=True),
    StructField("ap_mac_hash",       StringType(),    nullable=True),
    StructField("transmitter_mac_hash", StringType(), nullable=True),

    # Geospatial
    StructField("latitude",          DoubleType(),    nullable=True),
    StructField("longitude",         DoubleType(),    nullable=True),
    StructField("heading",           DoubleType(),    nullable=True),
    StructField("azimuth",           DoubleType(),    nullable=True),

    # Derived enrichment
    StructField("distance_est_m",    DoubleType(),    nullable=True),   # estimated from RSSI model
    StructField("is_randomized_mac", BooleanType(),   nullable=True),   # heuristic on bssid
    StructField("is_hidden_ssid",    BooleanType(),   nullable=True),

    # Quality
    StructField("confidence",        DoubleType(),    nullable=True),
    StructField("dq_flags",          ArrayType(StringType()), nullable=True),  # data quality flag list

    # Lineage
    StructField("pipeline_id",       StringType(),    nullable=True),
    StructField("source_path",       StringType(),    nullable=True),
])
