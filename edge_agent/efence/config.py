"""
E-Fence Edge Agent — Configuration
====================================
Loads config.yaml and resolves the Databricks token from the environment.
Falls back to safe defaults for optional fields.
"""

import os
import socket
import yaml
from pathlib import Path


def _get_pi_serial() -> str:
    """Read the Raspberry Pi CPU serial number from /proc/cpuinfo."""
    try:
        with open("/proc/cpuinfo", "r") as f:
            for line in f:
                if line.startswith("Serial"):
                    serial = line.split(":")[1].strip()
                    return "rpi-" + serial[-8:]
    except Exception:
        pass
    return "rpi-" + socket.gethostname()


def load(config_path: str = None) -> dict:
    """
    Load configuration from YAML file.

    Resolution order:
      1. config_path argument
      2. EFENCE_CONFIG env var
      3. ./config.yaml (relative to CWD)
      4. /etc/efence/config.yaml

    Databricks token is ALWAYS read from EFENCE_DATABRICKS_TOKEN env var.
    It is never stored in the config file.
    """
    search_paths = [
        config_path,
        os.environ.get("EFENCE_CONFIG"),
        os.path.join(os.path.dirname(__file__), "..", "config.yaml"),
        "/etc/efence/config.yaml",
    ]

    cfg = {}
    for path in search_paths:
        if path and Path(path).exists():
            with open(path, "r") as f:
                cfg = yaml.safe_load(f) or {}
            break

    # ---- Sensor identity ----
    sensor = cfg.setdefault("sensor", {})
    if not sensor.get("id"):
        sensor["id"] = _get_pi_serial()

    location = sensor.setdefault("location", {})
    location.setdefault("fixed", True)
    location.setdefault("latitude",  None)
    location.setdefault("longitude", None)

    # ---- Databricks ----
    db = cfg.setdefault("databricks", {})
    token = os.environ.get("EFENCE_DATABRICKS_TOKEN", "")
    if not token:
        raise RuntimeError(
            "EFENCE_DATABRICKS_TOKEN environment variable is not set. "
            "Export it before starting the agent:\n"
            "  export EFENCE_DATABRICKS_TOKEN='dapi...'"
        )
    db["token"] = token
    db.setdefault("workspace_url", "")
    db.setdefault("wifi_volume_path", "/Volumes/efence/wifi_raw/landing")
    db.setdefault("bt_volume_path",   "/Volumes/efence/bt_raw/landing")
    db.setdefault("upload_timeout_seconds", 30)
    db.setdefault("max_retries", 3)

    if not db["workspace_url"]:
        raise RuntimeError(
            "databricks.workspace_url is not set in config.yaml"
        )

    # ---- Wi-Fi ----
    wifi = cfg.setdefault("wifi", {})
    wifi.setdefault("enabled", True)
    wifi.setdefault("interface", "wlan0")
    wifi.setdefault("scan_interval_seconds", 15)
    wifi.setdefault("batch_size", 150)

    # ---- Bluetooth ----
    bt = cfg.setdefault("bluetooth", {})
    bt.setdefault("enabled", True)
    bt.setdefault("scan_duration_seconds", 8)
    bt.setdefault("scan_interval_seconds", 10)
    bt.setdefault("batch_size", 300)

    # ---- Buffer ----
    buf = cfg.setdefault("buffer", {})
    buf.setdefault("db_path", "/var/lib/efence/buffer.db")
    buf.setdefault("upload_interval_seconds", 60)
    buf.setdefault("max_records_per_upload", 500)
    buf.setdefault("retain_uploaded_days", 3)

    return cfg
