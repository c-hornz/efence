"""
E-Fence Edge Agent — Wi-Fi Scanner
=====================================
Scans for Wi-Fi networks and client probes using `iwlist scan`.

Works out of the box on Raspberry Pi OS without monitor mode.
Produces records matching the WIFI_BRONZE_SCHEMA exactly.

Fields populated by this scanner:
  sensor_id, event_time, message_id, bssid, ssid, channel,
  frequency_mhz, band, rssi_dbm, snr_db, encryption, auth_mode,
  vendor_oui, ap_mac, transmitter_mac, latitude, longitude,
  confidence, metadata

Fields left null (require monitor mode / pcap capture):
  frame_type, client_mac, heading, azimuth, raw_payload, capture_file
"""

import logging
import re
import subprocess
import uuid
from datetime import datetime, timezone
from typing import List, Optional

log = logging.getLogger("efence.wifi")

# Regex patterns for iwlist output parsing
_RE_ADDRESS  = re.compile(r"Address:\s+([0-9A-Fa-f:]{17})")
_RE_ESSID    = re.compile(r'ESSID:"(.*?)"')
_RE_CHANNEL  = re.compile(r"Channel:(\d+)")
_RE_FREQ     = re.compile(r"Frequency:([\d.]+)\s*GHz")
_RE_SIGNAL   = re.compile(r"Signal level=(-?\d+)\s*dBm")
_RE_NOISE    = re.compile(r"Noise level=(-?\d+)\s*dBm")
_RE_QUALITY  = re.compile(r"Quality=(\d+)/(\d+)")
_RE_ENC_ON   = re.compile(r"Encryption key:(on|off)")
_RE_WPA2     = re.compile(r"WPA2", re.IGNORECASE)
_RE_WPA3     = re.compile(r"WPA3|SAE|OWE", re.IGNORECASE)
_RE_WPA      = re.compile(r"WPA[^23]", re.IGNORECASE)
_RE_WEP      = re.compile(r"WEP", re.IGNORECASE)
_RE_AUTH     = re.compile(r"Authentication Suites.*?:\s*(.+)", re.IGNORECASE)
_RE_PSK      = re.compile(r"PSK", re.IGNORECASE)
_RE_EAP      = re.compile(r"802\.1x|EAP", re.IGNORECASE)
_RE_SAE      = re.compile(r"SAE", re.IGNORECASE)


def _freq_to_band(freq_ghz: float) -> Optional[str]:
    if 2.4 <= freq_ghz <= 2.5:
        return "2.4"
    elif 5.17 <= freq_ghz <= 5.83:
        return "5"
    elif 5.93 <= freq_ghz <= 7.13:
        return "6"
    return None


def _parse_encryption(cell_text: str) -> tuple:
    """Return (encryption_str, auth_mode_str) from a cell block."""
    enc_on = _RE_ENC_ON.search(cell_text)
    if not enc_on or enc_on.group(1) == "off":
        return ("OPEN", None)

    if _RE_WPA3.search(cell_text):
        enc = "WPA3"
        auth = "SAE" if _RE_SAE.search(cell_text) else "EAP"
    elif _RE_WPA2.search(cell_text):
        enc = "WPA2"
        auth = "EAP" if _RE_EAP.search(cell_text) else "PSK"
    elif _RE_WPA.search(cell_text):
        enc = "WPA"
        auth = "PSK"
    elif _RE_WEP.search(cell_text):
        enc = "WEP"
        auth = None
    else:
        enc = "UNKNOWN"
        auth = None

    return (enc, auth)


def _parse_iwlist_output(output: str, sensor_cfg: dict, event_time: datetime) -> List[dict]:
    """Parse raw `iwlist scan` output into a list of bronze-schema dicts."""
    records = []

    # Split on "Cell XX -" boundaries
    cells = re.split(r"Cell \d+ -", output)

    for cell in cells[1:]:  # skip the header before first Cell
        try:
            bssid_m  = _RE_ADDRESS.search(cell)
            freq_m   = _RE_FREQ.search(cell)
            signal_m = _RE_SIGNAL.search(cell)
            noise_m  = _RE_NOISE.search(cell)
            qual_m   = _RE_QUALITY.search(cell)
            chan_m   = _RE_CHANNEL.search(cell)
            essid_m  = _RE_ESSID.search(cell)

            if not bssid_m:
                continue

            bssid = bssid_m.group(1).lower()
            oui   = ":".join(bssid.split(":")[:3])

            freq_ghz  = float(freq_m.group(1))  if freq_m  else None
            freq_mhz  = int(freq_ghz * 1000)    if freq_ghz else None
            band      = _freq_to_band(freq_ghz)  if freq_ghz else None
            channel   = int(chan_m.group(1))     if chan_m   else None
            rssi      = int(signal_m.group(1))   if signal_m else None
            noise_dbm = int(noise_m.group(1))    if noise_m  else None
            snr       = round(rssi - noise_dbm, 1) if (rssi and noise_dbm) else None

            # Quality as confidence 0.0–1.0
            confidence = None
            if qual_m:
                confidence = round(int(qual_m.group(1)) / int(qual_m.group(2)), 3)

            ssid = essid_m.group(1) if essid_m else None
            if ssid == "":
                ssid = None  # hidden SSID

            enc, auth = _parse_encryption(cell)

            lat = sensor_cfg["location"].get("latitude")
            lon = sensor_cfg["location"].get("longitude")

            record = {
                "sensor_id":       sensor_cfg["id"],
                "event_time":      event_time.strftime("%Y-%m-%dT%H:%M:%S.") +
                                   f"{event_time.microsecond // 1000:03d}Z",
                "message_id":      str(uuid.uuid4()),
                "capture_file":    None,
                "bssid":           bssid,
                "ssid":            ssid,
                "channel":         channel,
                "frequency_mhz":   freq_mhz,
                "band":            band,
                "rssi_dbm":        rssi,
                "snr_db":          snr,
                "encryption":      enc,
                "auth_mode":       auth,
                "frame_type":      "beacon",   # iwlist only sees beacons
                "vendor_oui":      oui,
                "client_mac":      None,        # requires monitor mode
                "ap_mac":          bssid,
                "transmitter_mac": bssid,
                "latitude":        lat,
                "longitude":       lon,
                "heading":         None,
                "azimuth":         None,
                "confidence":      confidence,
                "raw_payload":     None,
                "metadata": {
                    "scanner":       "iwlist",
                    "firmware":      sensor_cfg.get("firmware", "unknown"),
                    "sensor_model":  sensor_cfg.get("model", "rpi-4b"),
                },
            }
            records.append(record)

        except Exception as exc:
            log.debug("Failed to parse cell: %s", exc)
            continue

    return records


class WiFiScanner:
    def __init__(self, cfg: dict):
        self._sensor_cfg = cfg["sensor"]
        self._interface  = cfg["wifi"]["interface"]

    def scan(self) -> List[dict]:
        """
        Run `iwlist <interface> scan` and return parsed bronze records.
        Requires wireless-tools to be installed:
          sudo apt-get install -y wireless-tools
        """
        event_time = datetime.now(timezone.utc)
        try:
            result = subprocess.run(
                ["iwlist", self._interface, "scan"],
                capture_output=True,
                text=True,
                timeout=20,
            )
            if result.returncode != 0:
                log.warning(
                    "iwlist scan returned %d: %s",
                    result.returncode,
                    result.stderr.strip()[:200],
                )
                return []

            records = _parse_iwlist_output(result.stdout, self._sensor_cfg, event_time)
            log.debug("Wi-Fi scan found %d networks on %s", len(records), self._interface)
            return records

        except FileNotFoundError:
            log.error(
                "iwlist not found. Install with: sudo apt-get install -y wireless-tools"
            )
            return []
        except subprocess.TimeoutExpired:
            log.warning("iwlist scan timed out on %s", self._interface)
            return []
        except Exception as exc:
            log.error("Wi-Fi scan error: %s", exc)
            return []
