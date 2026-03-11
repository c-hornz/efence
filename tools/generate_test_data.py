#!/usr/bin/env python3
"""
E-Fence Test Data Generator
============================
Generates realistic NDJSON test files that match the bronze-layer schemas for
the Wi-Fi and Bluetooth ingest pipelines exactly.

Each output file is newline-delimited JSON (one record per line), ready to be
dropped into the Auto Loader landing volumes:
  /Volumes/efence/wifi_raw/landing/
  /Volumes/efence/bt_raw/landing/

Usage:
  python generate_test_data.py [OPTIONS]

Examples:
  # Quick smoke test
  python generate_test_data.py --wifi-devices 5 --wifi-transmissions 50 \\
                               --bt-devices 8  --bt-transmissions 100

  # Large load test
  python generate_test_data.py --wifi-devices 50 --wifi-transmissions 10000 \\
                               --bt-devices 200 --bt-transmissions 50000 \\
                               --sensors 4 --batch-size 5000 --output-dir ./landing

  # Reproducible run
  python generate_test_data.py --wifi-devices 10 --wifi-transmissions 200 \\
                               --bt-devices 20  --bt-transmissions 400 \\
                               --seed 42 --duration-minutes 60

  # Inject intentional bad records for DQ testing
  python generate_test_data.py --wifi-devices 10 --wifi-transmissions 200 \\
                               --bt-devices 10  --bt-transmissions 200 \\
                               --fault-rate 0.05
"""

import argparse
import base64
import json
import math
import os
import random
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# REFERENCE DATA — realistic RF/protocol values
# =============================================================================

# --- Wi-Fi: channel → (frequency_mhz, band_str) ---
WIFI_CHANNELS: Dict[int, Tuple[int, str]] = {
    # 2.4 GHz
    1: (2412, "2.4"), 2: (2417, "2.4"), 3: (2422, "2.4"), 4: (2427, "2.4"),
    5: (2432, "2.4"), 6: (2437, "2.4"), 7: (2442, "2.4"), 8: (2447, "2.4"),
    9: (2452, "2.4"), 10: (2457, "2.4"), 11: (2462, "2.4"),
    # 5 GHz (UNII-1 through UNII-3)
    36: (5180, "5"),  40: (5200, "5"),  44: (5220, "5"),  48: (5240, "5"),
    52: (5260, "5"),  56: (5280, "5"),  60: (5300, "5"),  64: (5320, "5"),
    100:(5500, "5"), 104:(5520, "5"), 108:(5540, "5"), 112:(5560, "5"),
    116:(5580, "5"), 120:(5600, "5"), 124:(5620, "5"), 128:(5640, "5"),
    132:(5660, "5"), 136:(5680, "5"), 140:(5700, "5"), 144:(5720, "5"),
    149:(5745, "5"), 153:(5765, "5"), 157:(5785, "5"), 161:(5805, "5"), 165:(5825, "5"),
    # 6 GHz (Wi-Fi 6E)
    1+200: (5955, "6"), 5+200: (5975, "6"), 9+200: (5995, "6"), 13+200: (6015, "6"),
    17+200:(6035, "6"), 21+200:(6055, "6"), 25+200:(6075, "6"), 29+200:(6095, "6"),
}
WIFI_24_CHANNELS  = [1, 6, 11]          # most common non-overlapping
WIFI_5_CHANNELS   = [36, 40, 44, 48, 149, 153, 157, 161]
WIFI_6_CHANNELS   = [201, 205, 209, 213] # 6 GHz (mapped above)

# --- Wi-Fi: vendor OUI → manufacturer name ---
WIFI_OUIS: List[Tuple[str, str]] = [
    ("ac:87:a3", "Apple"),      ("f0:18:98", "Apple"),      ("8c:7b:9d", "Apple"),
    ("00:1e:65", "Intel"),      ("8c:ec:4b", "Intel"),      ("e0:94:67", "Intel"),
    ("50:c7:bf", "TP-Link"),    ("a0:f3:c1", "TP-Link"),    ("c4:6e:1f", "TP-Link"),
    ("20:4e:7f", "Netgear"),    ("00:09:5b", "Netgear"),
    ("04:92:26", "Asus"),       ("2c:fd:a1", "Asus"),
    ("00:01:42", "Cisco"),      ("00:0f:24", "Cisco"),
    ("78:1f:db", "Samsung"),    ("00:15:b9", "Samsung"),
    ("dc:a6:32", "Raspberry Pi"),("b8:27:eb", "Raspberry Pi"),
    ("00:26:37", "Qualcomm"),   ("0c:ee:e6", "Qualcomm"),
    ("a4:c3:f0", "Murata"),     ("80:e1:26", "Espressif"),
    ("cc:50:e3", "Espressif"),  ("24:62:ab", "Espressif"),
]

# --- Wi-Fi: realistic SSIDs ---
SSID_PREFIXES = [
    "NETGEAR", "Linksys", "TP-Link", "ASUS", "Xfinity",
    "ATT", "Spectrum", "CenturyLink", "Verizon", "FiOS",
]
SSID_NAMES = [
    "HomeNetwork", "FamilyWifi", "GuestNetwork", "SmartHome",
    "IoTNetwork", "CorporateAP", "OfficeWireless", "SecureNet",
    "Apartment5G", "BuildingWifi", "WarehouseNet", "FactoryFloor",
    "Surveillance", "Camera_AP", "PLC_Network",
]

# --- Wi-Fi: encryption / auth pairs ---
ENCRYPTION_PROFILES: List[Tuple[str, str, float]] = [
    # (encryption, auth_mode, probability)
    ("WPA2", "PSK",  0.50),
    ("WPA2", "EAP",  0.15),
    ("WPA3", "SAE",  0.15),
    ("WPA3", "EAP",  0.05),
    ("WPA",  "PSK",  0.05),
    ("OPEN", "",     0.07),
    ("WEP",  "",     0.03),
]

# --- Wi-Fi: frame types with realistic distribution ---
FRAME_TYPES_AP: List[Tuple[str, float]] = [
    ("beacon",        0.70),
    ("probe_resp",    0.15),
    ("assoc_resp",    0.08),
    ("auth",          0.04),
    ("deauth",        0.03),
]
FRAME_TYPES_CLIENT: List[Tuple[str, float]] = [
    ("probe_req",     0.55),
    ("data",          0.25),
    ("auth",          0.10),
    ("assoc_req",     0.07),
    ("deauth",        0.03),
]

# --- Bluetooth: manufacturer ID → (name, typical_tx_power, adv_profile) ---
# adv_profile: 'smartphone', 'fitness', 'beacon', 'classic', 'iot'
BT_MANUFACTURERS: List[Tuple[int, str, int, str]] = [
    (76,    "Apple",              4,   "smartphone"),
    (117,   "Samsung",            4,   "smartphone"),
    (6,     "Microsoft",          0,   "smartphone"),
    (224,   "Google",             0,   "smartphone"),
    (742,   "Fitbit",            -8,   "fitness"),
    (135,   "Garmin",            -8,   "fitness"),
    (107,   "Polar",             -8,   "fitness"),
    (89,    "Nordic Semiconductor", -4, "iot"),
    (1177,  "Ruuvi Innovations", -20,  "beacon"),
    (334,   "Kontakt.io",        -12,  "beacon"),
    (13249, "Tile",              -12,  "beacon"),
    (2,     "Intel",              0,   "smartphone"),
    (29,    "Broadcom",          -4,   "iot"),
    (1521,  "Espressif",         -4,   "iot"),
    (0,     "Unknown",           -8,   "iot"),
]

# --- BLE: service UUIDs (standard GATT profiles) ---
BLE_SERVICE_UUIDS: Dict[str, str] = {
    "0000180F-0000-1000-8000-00805F9B34FB": "Battery Service",
    "0000180D-0000-1000-8000-00805F9B34FB": "Heart Rate",
    "0000180A-0000-1000-8000-00805F9B34FB": "Device Information",
    "00001800-0000-1000-8000-00805F9B34FB": "Generic Access",
    "00001801-0000-1000-8000-00805F9B34FB": "Generic Attribute",
    "00001810-0000-1000-8000-00805F9B34FB": "Blood Pressure",
    "00001812-0000-1000-8000-00805F9B34FB": "Human Interface Device",
    "0000181A-0000-1000-8000-00805F9B34FB": "Environmental Sensing",
    "00001819-0000-1000-8000-00805F9B34FB": "Location and Navigation",
    "6E400001-B5A3-F393-E0A9-E50E24DCCA9E": "Nordic UART",
    "0000FE9F-0000-1000-8000-00805F9B34FB": "Google Nearby",
    "0000FD6F-0000-1000-8000-00805F9B34FB": "Apple Exposure Notification",
    "0000FFF0-0000-1000-8000-00805F9B34FB": "Common Custom Service",
}
BLE_SERVICE_UUID_LIST = list(BLE_SERVICE_UUIDS.keys())

# --- BLE: advertising types and their weights per device profile ---
ADV_TYPE_WEIGHTS: Dict[str, List[Tuple[str, float]]] = {
    "smartphone": [
        ("ADV_IND",       0.60),
        ("SCAN_RSP",      0.30),
        ("ADV_NONCONN_IND", 0.10),
    ],
    "fitness": [
        ("ADV_NONCONN_IND", 0.70),
        ("ADV_IND",         0.20),
        ("SCAN_RSP",        0.10),
    ],
    "beacon": [
        ("ADV_NONCONN_IND", 0.95),
        ("ADV_SCAN_IND",    0.05),
    ],
    "classic": [
        ("ADV_IND",         0.50),
        ("SCAN_RSP",        0.50),
    ],
    "iot": [
        ("ADV_NONCONN_IND", 0.60),
        ("ADV_IND",         0.30),
        ("ADV_SCAN_IND",    0.10),
    ],
}

# --- BLE: device name patterns by profile ---
BLE_DEVICE_NAMES: Dict[str, List[Optional[str]]] = {
    "smartphone": [
        "iPhone", "iPhone 14", "iPhone 15 Pro", "Galaxy S23", "Galaxy S24",
        "Pixel 7", "Pixel 8", "OnePlus 12", None, None,   # nulls: name not always advertised
    ],
    "fitness": [
        "Fitbit Charge 5", "Fitbit Versa 4", "Garmin Fenix 7", "Polar H10",
        "Polar Vantage V2", "Garmin Vivosmart 5", None,
    ],
    "beacon": [
        "RuuviTag", "Kontakt Beacon", "Tile Mate", "iBeacon", None, None,
    ],
    "classic": [
        "BT Headset", "JBL Flip 6", "Sony WH-1000XM5", "Bose QC45",
        "AirPods Pro", "Galaxy Buds 2", None,
    ],
    "iot": [
        "ESP32 Sensor", "nRF52 Node", "SmartLock", "TempSensor", None, None, None,
    ],
}

# --- BT OUIs (coarser — public addr prefix) ---
BT_OUIS: List[Tuple[str, str]] = [
    ("f8:27:93", "Apple"),    ("3c:6a:9d", "Apple"),    ("a4:c3:f0", "Murata/Apple"),
    ("00:1a:7d", "Intel"),    ("98:f2:b3", "Intel"),
    ("00:07:80", "Samsung"),  ("78:1f:db", "Samsung"),
    ("dc:a6:32", "Raspberry Pi"), ("b8:27:eb", "Raspberry Pi"),
    ("cc:50:e3", "Espressif"),("24:62:ab", "Espressif"),
    ("00:1b:dc", "Nordic"),   ("f4:ce:36", "Garmin"),
]

# --- Firmware versions for metadata ---
FIRMWARE_VERSIONS = ["1.0.0", "1.1.0", "1.2.0", "1.2.3", "2.0.0", "2.1.1", "2.3.0"]
ANTENNA_TYPES     = ["omni", "directional", "yagi", "patch", "dipole"]
SENSOR_MODELS     = ["rpi-4b", "rpi-3b+", "rpi-zero-2w", "rpi-cm4", "rpi-5"]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def weighted_choice(choices: List[Tuple[Any, float]], rng: random.Random) -> Any:
    """Select an item from (value, weight) pairs."""
    total = sum(w for _, w in choices)
    r = rng.uniform(0, total)
    cumulative = 0.0
    for value, weight in choices:
        cumulative += weight
        if r <= cumulative:
            return value
    return choices[-1][0]


def random_mac(rng: random.Random, oui: Optional[str] = None, randomized: bool = False) -> str:
    """
    Generate a MAC address.
    If oui provided, use it as prefix.
    If randomized=True, set the locally-administered bit (second nibble of first byte = 2/6/a/e).
    """
    if randomized:
        # Locally administered: set bit 1 of first byte
        first = rng.choice([0x02, 0x06, 0x0a, 0x0e, 0xc2, 0xc6, 0xca, 0xce])
        rest  = [rng.randint(0, 255) for _ in range(5)]
        return ":".join(f"{b:02x}" for b in [first] + rest)

    if oui:
        oui_bytes = oui.split(":")
        host_bytes = [rng.randint(0, 255) for _ in range(6 - len(oui_bytes))]
        all_bytes  = [int(b, 16) for b in oui_bytes] + host_bytes
        return ":".join(f"{b:02x}" for b in all_bytes)

    return ":".join(f"{rng.randint(0, 255):02x}" for _ in range(6))


def rssi_to_snr(rssi_dbm: int, rng: random.Random) -> float:
    """Estimate SNR from RSSI with realistic noise floor at ~-95 dBm."""
    noise_floor = -95 + rng.uniform(-3, 3)
    snr = rssi_dbm - noise_floor
    return round(max(1.0, snr + rng.uniform(-2, 2)), 1)


def jitter_rssi(base_rssi: int, rng: random.Random, sigma: float = 4.0) -> int:
    """Add realistic fading to a base RSSI value."""
    return max(-100, min(-20, int(base_rssi + rng.gauss(0, sigma))))


def jitter_coords(lat: float, lon: float, rng: random.Random,
                  fixed: bool = True) -> Tuple[float, float]:
    """
    For fixed sensors: tiny GPS jitter (~1m).
    For mobile sensors: larger drift (~50m).
    """
    spread = 0.000009 if fixed else 0.0005   # ~1m vs ~55m at equator
    return (
        round(lat + rng.uniform(-spread, spread), 7),
        round(lon + rng.uniform(-spread, spread), 7),
    )


def maybe_null(value: Any, rng: random.Random, null_rate: float) -> Any:
    """Return None with probability null_rate, otherwise return value."""
    return None if rng.random() < null_rate else value


def random_raw_payload(rng: random.Random, length_bytes: int = 24) -> str:
    """Generate a base64-encoded fake frame payload."""
    raw = bytes(rng.randint(0, 255) for _ in range(length_bytes))
    return base64.b64encode(raw).decode("ascii")


def random_hex_string(rng: random.Random, length_bytes: int) -> str:
    """Generate a hex string of given byte length."""
    return "".join(f"{rng.randint(0, 255):02x}" for _ in range(length_bytes))


def generate_message_id() -> str:
    return str(uuid.uuid4())


def ts_to_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def make_ssid(rng: random.Random, hidden_rate: float = 0.08) -> Optional[str]:
    """Generate a realistic SSID or None for hidden networks."""
    if rng.random() < hidden_rate:
        return None
    style = rng.randint(0, 2)
    if style == 0:
        return f"{rng.choice(SSID_PREFIXES)}{rng.randint(100, 9999)}"
    elif style == 1:
        return rng.choice(SSID_NAMES)
    else:
        return f"{rng.choice(SSID_NAMES)}-{rng.randint(1,99)}"


# =============================================================================
# DEVICE POOLS — stable per-device profiles
# =============================================================================

@dataclass
class WiFiDevice:
    """A persistent Wi-Fi network/device with stable RF characteristics."""
    device_id:       str
    device_type:     str        # "ap" or "client"
    bssid:           str        # stable MAC
    ssid:            Optional[str]
    channel:         int
    frequency_mhz:   int
    band:            str
    encryption:      str
    auth_mode:       str
    vendor_oui:      str
    base_rssi:       int        # mean RSSI this device is seen at
    is_randomized:   bool

    @classmethod
    def create(cls, rng: random.Random) -> "WiFiDevice":
        device_type   = rng.choices(["ap", "client"], weights=[0.6, 0.4])[0]
        is_randomized = device_type == "client" and rng.random() < 0.25
        oui_entry     = rng.choice(WIFI_OUIS)
        oui, _        = oui_entry

        bssid = random_mac(rng, oui=oui, randomized=is_randomized)
        ssid  = make_ssid(rng)

        # Channel / band selection
        band_choice = rng.choices(
            ["2.4", "5", "6"], weights=[0.35, 0.55, 0.10]
        )[0]
        if band_choice == "2.4":
            ch = rng.choice(WIFI_24_CHANNELS)
        elif band_choice == "5":
            ch = rng.choice(WIFI_5_CHANNELS)
        else:
            ch = rng.choice(WIFI_6_CHANNELS)
        freq_mhz, band_str = WIFI_CHANNELS[ch]

        enc, auth = weighted_choice(
            [((e, m), p) for e, m, p in ENCRYPTION_PROFILES],
            rng
        )

        # APs closer = stronger RSSI
        base_rssi = (
            rng.randint(-70, -30) if device_type == "ap"
            else rng.randint(-90, -45)
        )

        return cls(
            device_id    = f"wifi-{uuid.uuid4().hex[:8]}",
            device_type  = device_type,
            bssid        = bssid,
            ssid         = ssid,
            channel      = ch if ch < 200 else ch - 200,  # normalize 6GHz channel offset
            frequency_mhz= freq_mhz,
            band         = band_str,
            encryption   = enc,
            auth_mode    = auth,
            vendor_oui   = oui,
            base_rssi    = base_rssi,
            is_randomized= is_randomized,
        )


@dataclass
class BTDevice:
    """A persistent Bluetooth/BLE device with stable identity and profile."""
    device_id:       str
    profile:         str        # smartphone, fitness, beacon, classic, iot
    device_address:  str        # stable address
    is_randomized:   bool
    device_name:     Optional[str]
    manufacturer_id: int
    manufacturer_name: str
    vendor_oui:      Optional[str]
    advertising_uuid: Optional[str]
    service_uuids:   List[str]
    tx_power:        int
    base_rssi:       int
    protocol_subtype: str       # BLE or BT_CLASSIC

    @classmethod
    def create(cls, rng: random.Random) -> "BTDevice":
        mfr_entry      = rng.choice(BT_MANUFACTURERS)
        mfr_id, mfr_name, tx_pwr, profile = mfr_entry

        is_randomized  = profile == "smartphone" and rng.random() < 0.60
        oui_entry      = rng.choice(BT_OUIS) if not is_randomized else None
        oui            = oui_entry[0] if oui_entry else None
        addr           = random_mac(rng, oui=oui, randomized=is_randomized)

        device_name    = rng.choice(BLE_DEVICE_NAMES.get(profile, [None]))
        protocol       = "BT_CLASSIC" if profile == "classic" else "BLE"

        # Service UUIDs based on profile
        num_uuids = {
            "smartphone": rng.randint(1, 4),
            "fitness":    rng.randint(1, 3),
            "beacon":     rng.randint(0, 1),
            "classic":    0,
            "iot":        rng.randint(0, 2),
        }.get(profile, 0)
        svc_uuids = rng.sample(BLE_SERVICE_UUID_LIST, min(num_uuids, len(BLE_SERVICE_UUID_LIST)))

        adv_uuid = rng.choice(svc_uuids) if svc_uuids and rng.random() > 0.3 else None

        base_rssi = rng.randint(-90, -40)

        return cls(
            device_id       = f"bt-{uuid.uuid4().hex[:8]}",
            profile         = profile,
            device_address  = addr,
            is_randomized   = is_randomized,
            device_name     = device_name,
            manufacturer_id = mfr_id,
            manufacturer_name=mfr_name,
            vendor_oui      = oui,
            advertising_uuid= adv_uuid,
            service_uuids   = svc_uuids,
            tx_power        = tx_pwr,
            base_rssi       = base_rssi,
            protocol_subtype= protocol,
        )


@dataclass
class Sensor:
    """A fixed or mobile RF sensor (the Raspberry Pi)."""
    sensor_id: str
    latitude:  float
    longitude: float
    is_mobile: bool
    heading:   Optional[float]
    firmware:  str
    model:     str
    antenna:   str

    @classmethod
    def create(cls, index: int, rng: random.Random,
               base_lat: float = 37.7749, base_lon: float = -122.4194) -> "Sensor":
        is_mobile = rng.random() < 0.2
        lat = base_lat + rng.uniform(-0.005, 0.005) * (index + 1)
        lon = base_lon + rng.uniform(-0.005, 0.005) * (index + 1)
        heading = round(rng.uniform(0, 359.9), 1) if is_mobile else None
        return cls(
            sensor_id = f"rpi-sensor-{index + 1:02d}",
            latitude  = round(lat, 6),
            longitude = round(lon, 6),
            is_mobile = is_mobile,
            heading   = heading,
            firmware  = rng.choice(FIRMWARE_VERSIONS),
            model     = rng.choice(SENSOR_MODELS),
            antenna   = rng.choice(ANTENNA_TYPES),
        )


# =============================================================================
# RECORD GENERATORS
# =============================================================================

def generate_wifi_record(
    device: WiFiDevice,
    sensor: Sensor,
    event_time: datetime,
    rng: random.Random,
    capture_file: Optional[str],
    fault_rate: float = 0.0,
) -> Dict[str, Any]:
    """
    Generate one Wi-Fi bronze observation record matching WIFI_BRONZE_SCHEMA.
    All nullable fields are subject to realistic null rates and fault injection.
    """
    rssi    = jitter_rssi(device.base_rssi, rng)
    snr     = rssi_to_snr(rssi, rng)
    lat, lon= jitter_coords(sensor.latitude, sensor.longitude, rng, fixed=not sensor.is_mobile)

    # Frame type depends on device type
    frame_types = FRAME_TYPES_AP if device.device_type == "ap" else FRAME_TYPES_CLIENT
    frame_type  = weighted_choice(frame_types, rng)

    # Client MAC only for AP observations where a client is also captured
    client_mac = None
    if device.device_type == "ap" and frame_type in ("data", "assoc_resp") and rng.random() > 0.5:
        client_mac = random_mac(rng, randomized=rng.random() < 0.25)

    heading = None
    azimuth = None
    if sensor.is_mobile:
        heading = sensor.heading
        azimuth = round(rng.uniform(0, 359.9), 1) if rng.random() > 0.4 else None

    metadata: Dict[str, str] = {
        "firmware":      sensor.firmware,
        "sensor_model":  sensor.model,
        "antenna":       sensor.antenna,
    }
    if sensor.is_mobile:
        metadata["speed_kmh"] = str(round(rng.uniform(0, 50), 1))

    # ---- Fault injection for DQ testing ----
    is_fault = fault_rate > 0 and rng.random() < fault_rate

    record: Dict[str, Any] = {
        # Core identity (sensor_id and event_time are NOT nullable)
        "sensor_id":       sensor.sensor_id,
        "event_time":      ts_to_iso(event_time),
        "message_id":      maybe_null(generate_message_id(), rng, 0.02),
        "capture_file":    capture_file,

        # RF target
        "bssid":           maybe_null(device.bssid, rng, 0.01),
        "ssid":            device.ssid,   # already has hidden-SSID null logic
        "channel":         maybe_null(device.channel, rng, 0.03),
        "frequency_mhz":   maybe_null(device.frequency_mhz, rng, 0.03),
        "band":            maybe_null(device.band, rng, 0.03),

        # Signal
        "rssi_dbm":        maybe_null(rssi, rng, 0.04),
        "snr_db":          maybe_null(snr, rng, 0.10),

        # Security / protocol
        "encryption":      maybe_null(device.encryption, rng, 0.05),
        "auth_mode":       maybe_null(device.auth_mode if device.auth_mode else None, rng, 0.10),
        "frame_type":      maybe_null(frame_type, rng, 0.05),

        # Device identifiers
        "vendor_oui":      maybe_null(device.vendor_oui, rng, 0.08),
        "client_mac":      client_mac,
        "ap_mac":          maybe_null(device.bssid, rng, 0.02) if device.device_type == "ap" else None,
        "transmitter_mac": maybe_null(device.bssid, rng, 0.05),

        # Geospatial (some sensors fixed, no GPS)
        "latitude":        maybe_null(lat, rng, 0.10),
        "longitude":       maybe_null(lon, rng, 0.10),
        "heading":         heading,
        "azimuth":         azimuth,

        # Quality
        "confidence":      maybe_null(round(rng.uniform(0.55, 1.0), 3), rng, 0.05),

        # Raw / opaque
        "raw_payload":     maybe_null(random_raw_payload(rng, rng.randint(20, 64)), rng, 0.70),
        "metadata":        metadata,
    }

    # ---- Fault injection: introduce deliberately bad values ----
    if is_fault:
        fault_type = rng.choice(["null_sensor", "future_time", "bad_rssi", "missing_bssid"])
        if fault_type == "null_sensor":
            record["sensor_id"] = None
        elif fault_type == "future_time":
            future = event_time + timedelta(hours=rng.randint(1, 48))
            record["event_time"] = ts_to_iso(future)
        elif fault_type == "bad_rssi":
            record["rssi_dbm"] = rng.choice([10, 200, -200])  # physically impossible
        elif fault_type == "missing_bssid":
            record["bssid"] = None
            record["ap_mac"] = None

    return record


def generate_bt_record(
    device: BTDevice,
    sensor: Sensor,
    event_time: datetime,
    rng: random.Random,
    capture_file: Optional[str],
    fault_rate: float = 0.0,
) -> Dict[str, Any]:
    """
    Generate one Bluetooth bronze observation record matching BT_BRONZE_SCHEMA.
    """
    rssi     = jitter_rssi(device.base_rssi, rng, sigma=5.0)
    lat, lon = jitter_coords(sensor.latitude, sensor.longitude, rng, fixed=not sensor.is_mobile)

    adv_weights = ADV_TYPE_WEIGHTS.get(device.profile, ADV_TYPE_WEIGHTS["iot"])
    adv_type    = weighted_choice(adv_weights, rng)

    # tx_power: not always present in ADV packets
    tx_power = maybe_null(device.tx_power + rng.randint(-2, 2), rng, 0.35)

    # Manufacturer data: variable-length hex blob
    mfr_data_len = rng.randint(4, 16)
    mfr_data     = maybe_null(random_hex_string(rng, mfr_data_len), rng, 0.25)

    # Service UUIDs: sometimes subset is advertised
    if device.service_uuids:
        num_to_include = rng.randint(1, len(device.service_uuids))
        advertised_uuids = rng.sample(device.service_uuids, num_to_include)
    else:
        advertised_uuids = []

    heading = None
    azimuth = None
    if sensor.is_mobile:
        heading = sensor.heading
        azimuth = round(rng.uniform(0, 359.9), 1) if rng.random() > 0.4 else None

    metadata: Dict[str, str] = {
        "firmware":     sensor.firmware,
        "sensor_model": sensor.model,
        "hci_handle":   f"0x{rng.randint(0, 0xFFFF):04X}",
    }
    if device.profile == "beacon":
        metadata["beacon_type"] = rng.choice(["iBeacon", "Eddystone-URL", "Eddystone-UID", "AltBeacon"])

    is_fault = fault_rate > 0 and rng.random() < fault_rate

    record: Dict[str, Any] = {
        # Core identity
        "sensor_id":          sensor.sensor_id,
        "event_time":         ts_to_iso(event_time),
        "message_id":         maybe_null(generate_message_id(), rng, 0.02),
        "capture_file":       capture_file,

        # Device identity
        "device_address":     maybe_null(device.device_address, rng, 0.005),
        "address_randomized": maybe_null(device.is_randomized, rng, 0.15),
        "device_name":        device.device_name,  # already has null logic per profile
        "advertising_uuid":   maybe_null(device.advertising_uuid, rng, 0.40),

        # Manufacturer
        "manufacturer_id":    maybe_null(device.manufacturer_id, rng, 0.20),
        "manufacturer_data":  mfr_data,
        "service_uuids":      advertised_uuids if advertised_uuids else None,

        # Signal
        "rssi_dbm":           maybe_null(rssi, rng, 0.03),
        "tx_power":           tx_power,

        # Protocol
        "protocol_subtype":   device.protocol_subtype,
        "adv_type":           maybe_null(adv_type, rng, 0.04),
        "vendor_oui":         maybe_null(device.vendor_oui, rng, 0.15),

        # Geospatial
        "latitude":           maybe_null(lat, rng, 0.10),
        "longitude":          maybe_null(lon, rng, 0.10),
        "heading":            heading,
        "azimuth":            azimuth,

        # Quality
        "confidence":         maybe_null(round(rng.uniform(0.50, 1.0), 3), rng, 0.05),

        # Raw
        "raw_payload":        maybe_null(random_hex_string(rng, rng.randint(10, 64)), rng, 0.60),
        "metadata":           metadata,
    }

    if is_fault:
        fault_type = rng.choice(["null_sensor", "future_time", "bad_rssi", "null_address"])
        if fault_type == "null_sensor":
            record["sensor_id"] = None
        elif fault_type == "future_time":
            future = event_time + timedelta(hours=rng.randint(1, 48))
            record["event_time"] = ts_to_iso(future)
        elif fault_type == "bad_rssi":
            record["rssi_dbm"] = rng.choice([50, 300, -300])
        elif fault_type == "null_address":
            record["device_address"] = None

    return record


# =============================================================================
# FILE WRITER
# =============================================================================

def write_ndjson_batch(
    records: List[Dict[str, Any]],
    output_path: Path,
) -> int:
    """Write records as NDJSON (one JSON object per line). Returns bytes written."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total_bytes = 0
    with open(output_path, "w", encoding="utf-8") as fh:
        for rec in records:
            line = json.dumps(rec, separators=(",", ":")) + "\n"
            fh.write(line)
            total_bytes += len(line.encode("utf-8"))
    return total_bytes


# =============================================================================
# MAIN GENERATOR
# =============================================================================

def generate_timestamps(
    n: int,
    start: datetime,
    duration_minutes: int,
    rng: random.Random,
) -> List[datetime]:
    """
    Generate n timestamps spread across the time window.
    Uses a Poisson process to simulate realistic event inter-arrival times.
    """
    end_offset_s = duration_minutes * 60
    offsets = sorted(rng.uniform(0, end_offset_s) for _ in range(n))
    return [
        start + timedelta(seconds=s, microseconds=rng.randint(0, 999) * 1000)
        for s in offsets
    ]


def run_generator(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    start_time = (
        datetime.fromisoformat(args.start_time).replace(tzinfo=timezone.utc)
        if args.start_time
        else datetime.now(timezone.utc)
    )

    print(f"\nE-Fence Test Data Generator")
    print(f"{'─' * 50}")
    print(f"  Seed:             {args.seed}")
    print(f"  Start time:       {start_time.isoformat()}")
    print(f"  Duration:         {args.duration_minutes} minutes")
    print(f"  Sensors:          {args.sensors}")
    print(f"  Wi-Fi devices:    {args.wifi_devices}")
    print(f"  Wi-Fi tx:         {args.wifi_transmissions}")
    print(f"  BT devices:       {args.bt_devices}")
    print(f"  BT tx:            {args.bt_transmissions}")
    print(f"  Batch size:       {args.batch_size}")
    print(f"  Fault rate:       {args.fault_rate:.1%}")
    print(f"  Output dir:       {output_dir.resolve()}")
    print(f"{'─' * 50}\n")

    # ---- Build pools ----
    sensors = [Sensor.create(i, rng, args.base_lat, args.base_lon)
               for i in range(args.sensors)]

    wifi_devices = [WiFiDevice.create(rng) for _ in range(args.wifi_devices)]
    bt_devices   = [BTDevice.create(rng)   for _ in range(args.bt_devices)]

    # ---- Capture file names (one per pipeline run, shared across many records) ----
    wifi_capture = f"capture_wifi_{start_time.strftime('%Y%m%d_%H%M%S')}.pcap" if args.add_capture_file else None
    bt_capture   = None  # BT capture files less common

    # ---- Generate timestamps ----
    wifi_times = generate_timestamps(args.wifi_transmissions, start_time, args.duration_minutes, rng)
    bt_times   = generate_timestamps(args.bt_transmissions,  start_time, args.duration_minutes, rng)

    # ---- Generate Wi-Fi records ----
    print(f"Generating {args.wifi_transmissions:,} Wi-Fi transmissions...")
    wifi_records: List[Dict[str, Any]] = []
    for i, event_time in enumerate(wifi_times):
        device = wifi_devices[i % len(wifi_devices)]
        sensor = sensors[i % len(sensors)]
        rec = generate_wifi_record(
            device, sensor, event_time, rng,
            capture_file=wifi_capture,
            fault_rate=args.fault_rate,
        )
        wifi_records.append(rec)
        if (i + 1) % 10000 == 0:
            print(f"  Wi-Fi: {i + 1:,} / {args.wifi_transmissions:,}")

    # ---- Generate BT records ----
    print(f"Generating {args.bt_transmissions:,} Bluetooth transmissions...")
    bt_records: List[Dict[str, Any]] = []
    for i, event_time in enumerate(bt_times):
        device = bt_devices[i % len(bt_devices)]
        sensor = sensors[i % len(sensors)]
        rec = generate_bt_record(
            device, sensor, event_time, rng,
            capture_file=bt_capture,
            fault_rate=args.fault_rate,
        )
        bt_records.append(rec)
        if (i + 1) % 10000 == 0:
            print(f"  BT:   {i + 1:,} / {args.bt_transmissions:,}")

    # ---- Write output files ----
    run_tag  = start_time.strftime("%Y%m%d_%H%M%S")
    wifi_bytes_total = 0
    bt_bytes_total   = 0
    wifi_files       = []
    bt_files         = []

    if args.split_by_sensor:
        # One file per sensor per protocol
        for sensor in sensors:
            sid = sensor.sensor_id
            wifi_batch = [r for r in wifi_records if r["sensor_id"] == sid]
            bt_batch   = [r for r in bt_records   if r["sensor_id"] == sid]

            if wifi_batch:
                path = output_dir / "wifi" / f"wifi_{sid}_{run_tag}.json"
                wifi_bytes_total += write_ndjson_batch(wifi_batch, path)
                wifi_files.append(path)

            if bt_batch:
                path = output_dir / "bt" / f"bt_{sid}_{run_tag}.json"
                bt_bytes_total += write_ndjson_batch(bt_batch, path)
                bt_files.append(path)
    else:
        # Batch-size split files
        batch_size = args.batch_size

        for batch_idx, offset in enumerate(range(0, len(wifi_records), batch_size)):
            batch = wifi_records[offset : offset + batch_size]
            path  = output_dir / "wifi" / f"wifi_obs_{run_tag}_{batch_idx:04d}.json"
            wifi_bytes_total += write_ndjson_batch(batch, path)
            wifi_files.append(path)

        for batch_idx, offset in enumerate(range(0, len(bt_records), batch_size)):
            batch = bt_records[offset : offset + batch_size]
            path  = output_dir / "bt" / f"bt_obs_{run_tag}_{batch_idx:04d}.json"
            bt_bytes_total += write_ndjson_batch(batch, path)
            bt_files.append(path)

    # ---- Summary ----
    print(f"\n{'─' * 50}")
    print(f"  OUTPUT SUMMARY")
    print(f"{'─' * 50}")
    print(f"  Wi-Fi files:      {len(wifi_files):,}")
    print(f"  Wi-Fi records:    {len(wifi_records):,}")
    print(f"  Wi-Fi size:       {wifi_bytes_total / 1024:.1f} KB")
    print(f"  BT files:         {len(bt_files):,}")
    print(f"  BT records:       {len(bt_records):,}")
    print(f"  BT size:          {bt_bytes_total / 1024:.1f} KB")
    print(f"  Total records:    {len(wifi_records) + len(bt_records):,}")
    print(f"  Total size:       {(wifi_bytes_total + bt_bytes_total) / 1024:.1f} KB")

    if args.fault_rate > 0:
        expected_faults = int((len(wifi_records) + len(bt_records)) * args.fault_rate)
        print(f"  ~Fault records:   ~{expected_faults:,} ({args.fault_rate:.1%})")

    print(f"\n  Wi-Fi output dir: {(output_dir / 'wifi').resolve()}")
    print(f"  BT output dir:    {(output_dir / 'bt').resolve()}")
    print(f"{'─' * 50}\n")

    # ---- Print copy-paste commands for uploading to Databricks Volume ----
    if args.print_upload_cmds:
        print("Upload commands (Databricks CLI):")
        print(f"  databricks fs cp -r {(output_dir / 'wifi').resolve()} \\")
        print(f"    dbfs:/Volumes/efence/wifi_raw/landing/")
        print(f"  databricks fs cp -r {(output_dir / 'bt').resolve()} \\")
        print(f"    dbfs:/Volumes/efence/bt_raw/landing/")
        print()


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="E-Fence test data generator — produces NDJSON files for "
                    "Wi-Fi and Bluetooth bronze pipeline testing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ---- Volume controls ----
    vol = p.add_argument_group("Volume controls")
    vol.add_argument("--wifi-devices",       type=int, default=10,
                     help="Number of unique Wi-Fi APs/clients to simulate")
    vol.add_argument("--wifi-transmissions", type=int, default=200,
                     help="Total Wi-Fi observation records to generate")
    vol.add_argument("--bt-devices",         type=int, default=15,
                     help="Number of unique Bluetooth/BLE devices to simulate")
    vol.add_argument("--bt-transmissions",   type=int, default=400,
                     help="Total Bluetooth observation records to generate")
    vol.add_argument("--sensors",            type=int, default=2,
                     help="Number of sensors (Raspberry Pi units) to simulate")

    # ---- Time controls ----
    time_grp = p.add_argument_group("Time controls")
    time_grp.add_argument("--start-time",       type=str, default=None,
                           help="ISO8601 start timestamp, e.g. 2024-06-01T12:00:00 "
                                "(default: now)")
    time_grp.add_argument("--duration-minutes", type=int, default=30,
                           help="Time window in minutes to spread observations across")

    # ---- Output controls ----
    out = p.add_argument_group("Output controls")
    out.add_argument("--output-dir",        type=str, default="./test_output",
                     help="Directory for generated files (created if absent)")
    out.add_argument("--batch-size",        type=int, default=1000,
                     help="Records per output file (used when not splitting by sensor)")
    out.add_argument("--split-by-sensor",   action="store_true", default=False,
                     help="Write one file per sensor instead of batching by size")
    out.add_argument("--add-capture-file",  action="store_true", default=True,
                     help="Populate capture_file field with a simulated pcap filename")
    out.add_argument("--no-capture-file",   dest="add_capture_file", action="store_false",
                     help="Omit the capture_file field (leave null)")

    # ---- Geospatial ----
    geo = p.add_argument_group("Geospatial")
    geo.add_argument("--base-lat",  type=float, default=37.7749,
                     help="Base latitude for sensor cluster (default: San Francisco)")
    geo.add_argument("--base-lon",  type=float, default=-122.4194,
                     help="Base longitude for sensor cluster")

    # ---- Quality / fault injection ----
    dq = p.add_argument_group("Data quality / fault injection")
    dq.add_argument("--fault-rate", type=float, default=0.0,
                    help="Fraction of records to inject faults into (0.0–1.0). "
                         "Faults land in the quarantine table for DQ testing.")

    # ---- Reproducibility ----
    rep = p.add_argument_group("Reproducibility")
    rep.add_argument("--seed", type=int, default=None,
                     help="Random seed for reproducible output (default: random)")

    # ---- Misc ----
    misc = p.add_argument_group("Misc")
    misc.add_argument("--print-upload-cmds", action="store_true", default=False,
                      help="Print Databricks CLI upload commands after generation")

    return p


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    # Validation
    if args.wifi_devices < 1:
        parser.error("--wifi-devices must be >= 1")
    if args.bt_devices < 1:
        parser.error("--bt-devices must be >= 1")
    if args.sensors < 1:
        parser.error("--sensors must be >= 1")
    if args.wifi_transmissions < 0:
        parser.error("--wifi-transmissions must be >= 0")
    if args.bt_transmissions < 0:
        parser.error("--bt-transmissions must be >= 0")
    if not (0.0 <= args.fault_rate <= 1.0):
        parser.error("--fault-rate must be between 0.0 and 1.0")

    # Default seed to random if not set (but print it so runs are reproducible)
    if args.seed is None:
        args.seed = random.randint(0, 2**31 - 1)

    run_generator(args)


if __name__ == "__main__":
    main()
