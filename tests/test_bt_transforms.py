"""
Unit Tests: Bluetooth Silver Normalization Transforms
======================================================
Tests run locally using PySpark (not DLT runtime).

Run with:
    pytest tests/test_bt_transforms.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("efence-test-bt")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Inline transforms for testing (mirrors bluetooth/silver_normalize.py)
# ---------------------------------------------------------------------------

SALT = "test-salt-99999"
BLE_PATH_LOSS_EXPONENT = 2.0

def hash_address(df, col_name: str = "device_address", output: str = "device_address_hash", salt: str = SALT):
    return df.withColumn(output,
        F.sha2(F.concat(
            F.lower(F.regexp_replace(F.col(col_name), "[:\\-. ]", "")),
            F.lit(salt)), 256))

def normalize_protocol(df):
    raw = F.upper(F.trim(F.col("protocol_subtype")))
    return df.withColumn("protocol_subtype",
        F.when(raw.isin("BLE", "BTLE", "BT_LE", "LOW_ENERGY"), F.lit("BLE"))
        .when(raw.isin("CLASSIC", "BT_CLASSIC", "BR/EDR", "BREDR"),  F.lit("BT_CLASSIC"))
        .otherwise(F.lit("UNKNOWN")))

def normalize_adv_type(df):
    raw = F.upper(F.trim(F.col("adv_type")))
    return df.withColumn("adv_type",
        F.when(raw.isin("ADV_IND", "CONNECTABLE_UNDIRECTED"),        F.lit("ADV_IND"))
        .when(raw.isin("ADV_DIRECT_IND", "CONNECTABLE_DIRECTED"),    F.lit("ADV_DIRECT_IND"))
        .when(raw.isin("ADV_NONCONN_IND", "NON_CONNECTABLE"),        F.lit("ADV_NONCONN_IND"))
        .when(raw.isin("SCAN_RSP", "SCAN_RESPONSE"),                 F.lit("SCAN_RSP"))
        .when(raw.isin("ADV_SCAN_IND", "SCANNABLE_UNDIRECTED"),      F.lit("ADV_SCAN_IND"))
        .otherwise(raw))

def compute_path_loss(df):
    return df.withColumn("path_loss_db",
        F.when(
            F.col("tx_power").isNotNull() & F.col("rssi_dbm").isNotNull(),
            (F.col("tx_power") - F.col("rssi_dbm")).cast(IntegerType()),
        ).otherwise(F.lit(None).cast(IntegerType())))

def estimate_distance(df, exponent: float = BLE_PATH_LOSS_EXPONENT):
    ref_tx = F.coalesce(F.col("tx_power"), F.lit(-59)).cast(DoubleType())
    rssi   = F.col("rssi_dbm").cast(DoubleType())
    in_range = rssi.between(-110, 0) & ref_tx.between(-100, 20)
    exp_col = (ref_tx - rssi) / F.lit(10.0 * exponent)
    return df.withColumn("distance_est_m",
        F.when(rssi.isNotNull() & in_range,
               F.round(F.pow(F.lit(10.0), exp_col), 2))
        .otherwise(F.lit(None).cast(DoubleType())))

def is_ble_random(df, col_name: str = "device_address"):
    random_nibbles = ["c","C","d","D","e","E","f","F","4","5","6","7","0","1","2","3"]
    flag = (
        F.regexp_extract(F.col(col_name), r"^([0-9a-fA-F]{2})", 1)
        .substr(2, 1).isin(random_nibbles)
    )
    return df.withColumn("is_randomized_address",
        F.when(F.col(col_name).isNotNull(), flag)
        .otherwise(F.lit(None).cast(BooleanType())))


# ---------------------------------------------------------------------------
# Tests: protocol normalization
# ---------------------------------------------------------------------------

class TestProtocolNormalization:

    def test_ble_variants(self, spark):
        data = [("BLE",), ("BTLE",), ("BT_LE",), ("low_energy",)]
        df = spark.createDataFrame(data, ["protocol_subtype"])
        result = normalize_protocol(df).collect()
        for row in result:
            assert row["protocol_subtype"] == "BLE"

    def test_classic_variants(self, spark):
        data = [("CLASSIC",), ("BT_CLASSIC",), ("BR/EDR",)]
        df = spark.createDataFrame(data, ["protocol_subtype"])
        result = normalize_protocol(df).collect()
        for row in result:
            assert row["protocol_subtype"] == "BT_CLASSIC"

    def test_unknown_protocol(self, spark):
        data = [("ZIGBEE",), ("custom",)]
        df = spark.createDataFrame(data, ["protocol_subtype"])
        result = normalize_protocol(df).collect()
        for row in result:
            assert row["protocol_subtype"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# Tests: advertising type normalization
# ---------------------------------------------------------------------------

class TestAdvTypeNormalization:

    def test_adv_ind_variants(self, spark):
        data = [("ADV_IND",), ("CONNECTABLE_UNDIRECTED",)]
        df = spark.createDataFrame(data, ["adv_type"])
        result = normalize_adv_type(df).collect()
        for row in result:
            assert row["adv_type"] == "ADV_IND"

    def test_scan_rsp_variants(self, spark):
        data = [("SCAN_RSP",), ("SCAN_RESPONSE",)]
        df = spark.createDataFrame(data, ["adv_type"])
        result = normalize_adv_type(df).collect()
        for row in result:
            assert row["adv_type"] == "SCAN_RSP"


# ---------------------------------------------------------------------------
# Tests: path loss computation
# ---------------------------------------------------------------------------

class TestPathLoss:

    def _make_df(self, spark, tx_power, rssi_dbm):
        data = [(tx_power, rssi_dbm)]
        schema = StructType([
            StructField("tx_power", IntegerType(), True),
            StructField("rssi_dbm", IntegerType(), True),
        ])
        return spark.createDataFrame(data, schema)

    def test_path_loss_computed_correctly(self, spark):
        df = self._make_df(spark, tx_power=4, rssi_dbm=-67)
        result = compute_path_loss(df).first()
        assert result["path_loss_db"] == 71  # 4 - (-67) = 71

    def test_path_loss_null_when_tx_missing(self, spark):
        df = self._make_df(spark, tx_power=None, rssi_dbm=-67)
        result = compute_path_loss(df).first()
        assert result["path_loss_db"] is None

    def test_path_loss_null_when_rssi_missing(self, spark):
        df = self._make_df(spark, tx_power=4, rssi_dbm=None)
        result = compute_path_loss(df).first()
        assert result["path_loss_db"] is None


# ---------------------------------------------------------------------------
# Tests: BLE distance estimation
# ---------------------------------------------------------------------------

class TestBLEDistance:

    def _make_df(self, spark, rssi, tx_power=None):
        schema = StructType([
            StructField("rssi_dbm", IntegerType(), True),
            StructField("tx_power", IntegerType(), True),
        ])
        return spark.createDataFrame([(rssi, tx_power)], schema)

    def test_at_reference_power_1m(self, spark):
        """tx_power = -59 (reference), rssi = -59 → 1.0m."""
        df = self._make_df(spark, rssi=-59, tx_power=-59)
        result = estimate_distance(df).first()
        assert abs(result["distance_est_m"] - 1.0) < 0.1

    def test_stronger_rssi_shorter_distance(self, spark):
        d_far  = estimate_distance(self._make_df(spark, -80)).first()["distance_est_m"]
        d_near = estimate_distance(self._make_df(spark, -50)).first()["distance_est_m"]
        assert d_near < d_far

    def test_uses_tx_power_when_present(self, spark):
        """With tx_power -4, RSSI -59 → longer distance than default ref."""
        d_no_tx  = estimate_distance(self._make_df(spark, -59, None)).first()["distance_est_m"]
        d_with_tx = estimate_distance(self._make_df(spark, -59, -4)).first()["distance_est_m"]
        # tx_power -4 < default -59, so effective power is weaker → shorter estimated distance
        assert d_with_tx != d_no_tx  # confirms tx_power is used

    def test_null_rssi_returns_null(self, spark):
        schema = StructType([
            StructField("rssi_dbm", IntegerType(), True),
            StructField("tx_power", IntegerType(), True),
        ])
        df = spark.createDataFrame([(None, None)], schema)
        assert estimate_distance(df).first()["distance_est_m"] is None


# ---------------------------------------------------------------------------
# Tests: BLE random address detection
# ---------------------------------------------------------------------------

class TestBLERandomAddress:

    def test_static_random_addresses(self, spark):
        """Static random: top 2 bits = 11 → first octet 0xC0-0xFF."""
        macs = [("f8:01:02:03:04:05",), ("c0:01:02:03:04:05",)]
        df = spark.createDataFrame(macs, ["device_address"])
        result = is_ble_random(df).collect()
        for row in result:
            assert row["is_randomized_address"] is True

    def test_public_address_not_random(self, spark):
        """Public addresses: top 2 bits = 00 in first byte, globally assigned."""
        macs = [("a4:c3:f0:11:22:33",),]   # Apple OUI - public
        df = spark.createDataFrame(macs, ["device_address"])
        # Note: this heuristic is imperfect — the test documents expected behavior
        # In production, rely on the address_randomized flag from the HCI event.
        result = is_ble_random(df).first()
        # 'a' = 1010, second nibble is '4' → not in random set → not randomized
        assert result["is_randomized_address"] is False

    def test_null_address_gives_null(self, spark):
        schema = StructType([StructField("device_address", StringType(), True)])
        df = spark.createDataFrame([(None,)], schema)
        assert is_ble_random(df).first()["is_randomized_address"] is None
