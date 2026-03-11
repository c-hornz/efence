"""
E-Fence Edge Agent — Bluetooth/BLE Scanner
============================================
Scans for BLE advertisements using bleak 0.14.3 (Python 3.7 compatible).
Produces records matching the BT_BRONZE_SCHEMA exactly.

Fields populated:
  sensor_id, event_time, message_id, device_address, address_randomized,
  device_name, advertising_uuid, manufacturer_id, manufacturer_data,
  service_uuids, rssi_dbm, tx_power, protocol_subtype, adv_type,
  vendor_oui, latitude, longitude, confidence, metadata

Requires: pip3 install bleak==0.14.3
The BlueZ stack must be running: sudo systemctl start bluetooth
"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("efence.bt")

# BLE random address detection: second nibble of first octet
_RANDOM_NIBBLES = set("2367abcdefABCDEF")

# BT SIG company ID → human name (subset for common devices)
_MANUFACTURER_NAMES: Dict[int, str] = {
    6:     "Microsoft",
    76:    "Apple",
    89:    "Nordic Semiconductor",
    107:   "Polar",
    117:   "Samsung",
    135:   "Garmin",
    224:   "Google",
    334:   "Kontakt.io",
    742:   "Fitbit",
    1177:  "Ruuvi Innovations",
    1521:  "Espressif",
    13249: "Tile",
}


def _is_random_address(address: str) -> bool:
    """
    Heuristic: BLE random addresses have the locally-administered bit set.
    Second hex char of the first octet in {2,6,a,b,c,d,e,f,A,B,C,D,E,F}.
    """
    try:
        return address[1].upper() in _RANDOM_NIBBLES
    except IndexError:
        return False


def _oui(address: str) -> Optional[str]:
    """Extract the OUI (first 3 octets) from a MAC address."""
    parts = address.split(":")
    if len(parts) >= 3:
        return ":".join(parts[:3]).lower()
    return None


def _adv_type_from_flags(advertisement_data) -> str:
    """
    Infer BLE advertising type from advertisement data flags.
    bleak 0.14.x does not expose adv type directly; we infer it.
    """
    # If connectable flag present in local_name or service data, assume ADV_IND
    # Otherwise default to ADV_NONCONN_IND for scanners.
    # Connectable = advertisement_data has service_data or local_name
    has_name = bool(getattr(advertisement_data, "local_name", None))
    has_services = bool(getattr(advertisement_data, "service_uuids", None))

    if has_name or has_services:
        return "ADV_IND"
    return "ADV_NONCONN_IND"


class BTScanner:
    def __init__(self, cfg: dict):
        self._sensor_cfg      = cfg["sensor"]
        self._scan_duration   = cfg["bluetooth"]["scan_duration_seconds"]
        self._collected: List[dict] = []

    def _make_record(self, device, advertisement_data, event_time: datetime) -> dict:
        """Convert a bleak device + advertisement into a BT_BRONZE_SCHEMA record."""
        address  = device.address.lower()
        is_rand  = _is_random_address(address)

        # Manufacturer data: {company_id (int): bytes}
        mfr_id   = None
        mfr_data = None
        mfr_name = None
        mfr_dict = getattr(advertisement_data, "manufacturer_data", {}) or {}
        if mfr_dict:
            mfr_id   = next(iter(mfr_dict))
            mfr_data = mfr_dict[mfr_id].hex()
            mfr_name = _MANUFACTURER_NAMES.get(mfr_id)

        # Service UUIDs
        svc_uuids = list(getattr(advertisement_data, "service_uuids", []) or [])
        adv_uuid  = svc_uuids[0].upper() if svc_uuids else None

        # TX power
        tx_power = getattr(advertisement_data, "tx_power", None)

        # RSSI
        rssi = getattr(device, "rssi", None)

        # Device name
        name = (
            getattr(advertisement_data, "local_name", None)
            or getattr(device, "name", None)
        )

        # Confidence: derived from RSSI (stronger signal = more reliable read)
        confidence = None
        if rssi is not None:
            confidence = round(min(1.0, max(0.3, (rssi + 100) / 80)), 3)

        lat = self._sensor_cfg["location"].get("latitude")
        lon = self._sensor_cfg["location"].get("longitude")

        return {
            "sensor_id":          self._sensor_cfg["id"],
            "event_time":         event_time.strftime("%Y-%m-%dT%H:%M:%S.") +
                                  f"{event_time.microsecond // 1000:03d}Z",
            "message_id":         str(uuid.uuid4()),
            "capture_file":       None,
            "device_address":     address,
            "address_randomized": is_rand,
            "device_name":        name,
            "advertising_uuid":   adv_uuid,
            "manufacturer_id":    mfr_id,
            "manufacturer_data":  mfr_data,
            "service_uuids":      svc_uuids if svc_uuids else None,
            "rssi_dbm":           rssi,
            "tx_power":           tx_power,
            "protocol_subtype":   "BLE",
            "adv_type":           _adv_type_from_flags(advertisement_data),
            "vendor_oui":         None if is_rand else _oui(address),
            "latitude":           lat,
            "longitude":          lon,
            "heading":            None,
            "azimuth":            None,
            "confidence":         confidence,
            "raw_payload":        None,
            "metadata": {
                "scanner":        "bleak",
                "manufacturer":   mfr_name,
                "firmware":       self._sensor_cfg.get("firmware", "unknown"),
                "sensor_model":   self._sensor_cfg.get("model", "rpi-4b"),
            },
        }

    async def scan_async(self) -> List[dict]:
        """
        Run a BLE scan for `scan_duration` seconds.
        Returns all advertisement records collected during the window.
        """
        try:
            from bleak import BleakScanner
        except ImportError:
            log.error("bleak not installed. Run: pip3 install bleak==0.14.3")
            return []

        records:  List[dict]          = []
        seen:     Dict[str, datetime] = {}   # address → first seen this scan

        def _callback(device, advertisement_data):
            # Deduplicate within the scan window — keep one record per address
            # per scan (BLE devices re-advertise every 20ms–10s)
            addr = device.address.lower()
            if addr not in seen:
                seen[addr] = datetime.now(timezone.utc)
                try:
                    rec = self._make_record(device, advertisement_data, seen[addr])
                    records.append(rec)
                except Exception as exc:
                    log.debug("Failed to parse BLE advertisement: %s", exc)

        try:
            async with BleakScanner(detection_callback=_callback):
                await asyncio.sleep(self._scan_duration)
        except Exception as exc:
            log.error("BLE scan error: %s", exc)

        log.debug("BLE scan found %d unique devices", len(records))
        return records

    def scan(self) -> List[dict]:
        """Synchronous wrapper around scan_async for use in threads."""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(self.scan_async())
        finally:
            loop.close()
