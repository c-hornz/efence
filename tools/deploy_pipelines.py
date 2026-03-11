"""
E-Fence Databricks Pipeline Deployer
======================================
1. Connects the GitHub repo to Databricks Repos (workspace files)
2. Creates / updates all three DLT pipelines via the Pipelines REST API
3. Triggers the wifi and bluetooth ingest pipelines

Usage:
  export DATABRICKS_TOKEN="dapi..."
  python3 tools/deploy_pipelines.py --workspace https://dbc-f22c6f69-7e5f.cloud.databricks.com

  # Trigger gold pipeline too
  python3 tools/deploy_pipelines.py --workspace https://dbc-f22c6f69-7e5f.cloud.databricks.com --all

  # Dry run (print what would be created, no API calls)
  python3 tools/deploy_pipelines.py --workspace https://... --dry-run
"""

import argparse
import json
import os
import sys
import time
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

WORKSPACE_URL  = None   # set via --workspace arg
TOKEN          = None   # set via DATABRICKS_TOKEN env var
REPO_PATH      = "/Repos/efence/efence"
GITHUB_URL     = "https://github.com/c-hornz/efence.git"
CATALOG        = "efence_dev"

PIPELINE_DEFS = [
    {
        "key":     "wifi",
        "name":    "efence-wifi-ingest",
        "catalog": CATALOG,
        "target":  "wifi_silver",
        "libraries": [
            {"file": {"path": f"{REPO_PATH}/pipelines/wifi/bronze_ingest.py"}},
            {"file": {"path": f"{REPO_PATH}/pipelines/wifi/silver_normalize.py"}},
        ],
        "configuration": {
            "efence.wifi.landing_path":           f"/Volumes/{CATALOG}/wifi_raw/landing",
            "efence.wifi.schema_location":        f"/Volumes/{CATALOG}/wifi_raw/_autoloader_schema",
            "efence.mac.hash.salt":               "{{secrets/efence-secrets/mac-hash-salt}}",
            "efence.wifi.dedup_watermark_hours":  "2",
            "efence.wifi.late_arrival_drop_days": "7",
            "spark.sql.streaming.statefulOperator.checkCorrectness.enabled": "false",
        },
        "trigger_on_deploy": True,
    },
    {
        "key":     "bt",
        "name":    "efence-bluetooth-ingest",
        "catalog": CATALOG,
        "target":  "bt_silver",
        "libraries": [
            {"file": {"path": f"{REPO_PATH}/pipelines/bluetooth/bronze_ingest.py"}},
            {"file": {"path": f"{REPO_PATH}/pipelines/bluetooth/silver_normalize.py"}},
        ],
        "configuration": {
            "efence.bt.landing_path":            f"/Volumes/{CATALOG}/bt_raw/landing",
            "efence.bt.schema_location":         f"/Volumes/{CATALOG}/bt_raw/_autoloader_schema",
            "efence.mac.hash.salt":              "{{secrets/efence-secrets/mac-hash-salt}}",
            "efence.bt.dedup_watermark_hours":   "1",
            "efence.bt.late_arrival_drop_days":  "7",
            "spark.sql.streaming.statefulOperator.checkCorrectness.enabled": "false",
        },
        "trigger_on_deploy": True,
    },
    {
        "key":     "gold",
        "name":    "efence-gold-analytics",
        "catalog": CATALOG,
        "target":  "gold",
        "libraries": [
            {"file": {"path": f"{REPO_PATH}/pipelines/gold/correlation.py"}},
        ],
        "configuration": {
            "efence.gold.copresence_window_seconds": "30",
            "efence.gold.proximity_threshold_m":     "50.0",
            "efence.gold.alert_rssi_threshold_dbm":  "-65",
            "efence.gold.alert_min_confidence":      "0.7",
        },
        "trigger_on_deploy": False,  # triggered manually or by --all
    },
]

CLUSTER_CONFIG = {
    "label": "default",
    "autoscale": {
        "min_workers": 1,
        "max_workers": 3,
        "mode": "ENHANCED",
    },
    # spark_version is NOT allowed in DLT cluster config — DLT manages its own runtime
}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _headers():
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type":  "application/json",
    }


def _get(path: str) -> dict:
    resp = requests.get(f"{WORKSPACE_URL}{path}", headers=_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def _post(path: str, body: dict) -> dict:
    resp = requests.post(
        f"{WORKSPACE_URL}{path}",
        headers=_headers(),
        json=body,
        timeout=30,
    )
    if not resp.ok:
        print(f"  ERROR {resp.status_code}: {resp.text[:400]}")
        resp.raise_for_status()
    return resp.json()


def _patch(path: str, body: dict) -> dict:
    resp = requests.patch(
        f"{WORKSPACE_URL}{path}",
        headers=_headers(),
        json=body,
        timeout=30,
    )
    if not resp.ok:
        print(f"  ERROR {resp.status_code}: {resp.text[:400]}")
        resp.raise_for_status()
    return resp.json()


def _put(path: str, body: dict) -> dict:
    resp = requests.put(
        f"{WORKSPACE_URL}{path}",
        headers=_headers(),
        json=body,
        timeout=30,
    )
    if not resp.ok:
        print(f"  ERROR {resp.status_code}: {resp.text[:400]}")
        resp.raise_for_status()
    return {} if not resp.content else resp.json()


# ---------------------------------------------------------------------------
# Step 1: Ensure GitHub repo is connected in Databricks Repos
# ---------------------------------------------------------------------------

def ensure_repo(dry_run: bool) -> bool:
    print(f"\n[1/3] Checking Databricks Repo at {REPO_PATH}...")

    # Check if repo already exists
    try:
        existing = _get(f"/api/2.0/repos?path_prefix={REPO_PATH}")
        repos = existing.get("repos", [])
        if repos:
            print(f"  ✓ Repo already connected: {repos[0]['path']}")
            return True
    except Exception:
        pass

    print(f"  Connecting {GITHUB_URL} → {REPO_PATH}")
    if dry_run:
        print("  [DRY RUN] Would POST /api/2.0/repos")
        return True

    # Ensure the parent folder exists before creating the repo
    parent_folder = "/".join(REPO_PATH.rstrip("/").split("/")[:-1])  # e.g. /Repos/efence
    try:
        _post("/api/2.0/workspace/mkdirs", {"path": parent_folder})
        print(f"  ✓ Parent folder ready: {parent_folder}")
    except Exception as exc:
        print(f"  Note: mkdirs returned: {exc} (may already exist)")

    try:
        result = _post("/api/2.0/repos", {
            "url":      GITHUB_URL,
            "provider": "github",
            "path":     REPO_PATH,
        })
        print(f"  ✓ Repo connected (id={result.get('id')})")
        return True
    except Exception as exc:
        print(f"  ✗ Failed to connect repo: {exc}")
        print(f"  → Connect manually: Workspace → Repos → Add Repo → {GITHUB_URL}")
        return False


# ---------------------------------------------------------------------------
# Step 2: Create or update DLT pipelines
# ---------------------------------------------------------------------------

def _list_pipelines() -> dict:
    """Return {pipeline_name: pipeline_id} for all existing pipelines."""
    result = {}
    token  = None
    while True:
        params = {"max_results": 100}
        if token:
            params["page_token"] = token
        resp = requests.get(
            f"{WORKSPACE_URL}/api/2.0/pipelines",
            headers=_headers(),
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        for p in data.get("statuses", []):
            result[p["name"]] = p["pipeline_id"]
        token = data.get("next_page_token")
        if not token:
            break
    return result


def _build_pipeline_body(defn: dict) -> dict:
    return {
        "name":          defn["name"],
        "catalog":       defn["catalog"],
        "target":        defn["target"],
        "libraries":     defn["libraries"],
        "configuration": defn["configuration"],
        "serverless":    True,   # workspace requires serverless compute
        "continuous":    False,
        "development":   False,
        "channel":       "CURRENT",
        "edition":       "ADVANCED",
    }


def deploy_pipelines(dry_run: bool, trigger_gold: bool) -> dict:
    """Create or update all pipelines. Returns {key: pipeline_id}."""
    print("\n[2/3] Deploying DLT pipelines...")

    existing = _list_pipelines()
    pipeline_ids = {}

    for defn in PIPELINE_DEFS:
        name = defn["name"]
        body = _build_pipeline_body(defn)

        if name in existing:
            pid = existing[name]
            print(f"  Updating existing pipeline: {name} (id={pid})")
            if not dry_run:
                _put(f"/api/2.0/pipelines/{pid}", body)
                print(f"  ✓ Updated")
            else:
                print(f"  [DRY RUN] Would PUT /api/2.0/pipelines/{pid}")
            pipeline_ids[defn["key"]] = pid
        else:
            print(f"  Creating new pipeline: {name}")
            if not dry_run:
                result = _post("/api/2.0/pipelines", body)
                pid = result["pipeline_id"]
                print(f"  ✓ Created (id={pid})")
                pipeline_ids[defn["key"]] = pid
            else:
                print(f"  [DRY RUN] Would POST /api/2.0/pipelines")
                pipeline_ids[defn["key"]] = f"dry-run-{defn['key']}"

    return pipeline_ids


# ---------------------------------------------------------------------------
# Step 3: Trigger pipeline updates
# ---------------------------------------------------------------------------

def _stop_active_update(pipeline_id: str):
    """Stop any active update on the pipeline so a new one can start."""
    try:
        info = _get(f"/api/2.0/pipelines/{pipeline_id}")
        update_id = info.get("latest_updates", [{}])[0].get("update_id")
        state = info.get("latest_updates", [{}])[0].get("state", "")
        if state in ("CREATED", "WAITING_FOR_RESOURCES", "INITIALIZING", "RUNNING",
                      "SETTING_UP_TABLES", "RESETTING"):
            print(f"  Stopping active update {update_id} (state={state})...")
            _post(f"/api/2.0/pipelines/{pipeline_id}/stop", {})
            for _ in range(30):
                time.sleep(2)
                info = _get(f"/api/2.0/pipelines/{pipeline_id}")
                cur = info.get("state", "")
                if cur in ("IDLE", "FAILED"):
                    print(f"  ✓ Pipeline now {cur}")
                    return
            print("  WARNING: Timed out waiting for pipeline to stop")
    except Exception as exc:
        print(f"  WARNING: Could not stop active update: {exc}")


def trigger_pipeline(pipeline_id: str, name: str, dry_run: bool) -> str:
    """Start a pipeline update. Returns the update_id."""
    print(f"  Triggering: {name} (id={pipeline_id})")
    if dry_run:
        print(f"  [DRY RUN] Would POST /api/2.0/pipelines/{pipeline_id}/updates")
        return "dry-run-update-id"

    try:
        result = _post(f"/api/2.0/pipelines/{pipeline_id}/updates", {
            "full_refresh": False,
        })
    except requests.exceptions.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 409:
            _stop_active_update(pipeline_id)
            result = _post(f"/api/2.0/pipelines/{pipeline_id}/updates", {
                "full_refresh": False,
            })
        else:
            raise

    update_id = result.get("update_id", "unknown")
    url = f"{WORKSPACE_URL}/#joblist/pipelines/{pipeline_id}"
    print(f"  ✓ Update started (update_id={update_id})")
    print(f"  → Monitor: {url}")
    return update_id


def trigger_pipelines(pipeline_ids: dict, dry_run: bool, trigger_gold: bool):
    print("\n[3/3] Triggering pipeline updates...")

    for defn in PIPELINE_DEFS:
        key = defn["key"]
        pid = pipeline_ids.get(key)
        if not pid:
            print(f"  Skipping {defn['name']} — no pipeline ID")
            continue

        should_trigger = defn["trigger_on_deploy"] or (trigger_gold and key == "gold")
        if should_trigger:
            trigger_pipeline(pid, defn["name"], dry_run)
        else:
            print(f"  Skipping {defn['name']} (use --all to trigger gold)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global WORKSPACE_URL, TOKEN

    parser = argparse.ArgumentParser(description="Deploy E-Fence DLT pipelines to Databricks")
    parser.add_argument("--workspace", required=True,
                        help="Databricks workspace URL, e.g. https://dbc-xxx.cloud.databricks.com")
    parser.add_argument("--all",      action="store_true", default=False,
                        help="Also trigger the gold correlation pipeline")
    parser.add_argument("--dry-run",  action="store_true", default=False,
                        help="Print what would happen without making API calls")
    args = parser.parse_args()

    WORKSPACE_URL = args.workspace.rstrip("/")
    TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

    if not TOKEN:
        print("ERROR: DATABRICKS_TOKEN environment variable not set.")
        print("  export DATABRICKS_TOKEN='dapi...'")
        sys.exit(1)

    if args.dry_run:
        print("=== DRY RUN MODE — no changes will be made ===")

    print(f"\nE-Fence Pipeline Deployer")
    print(f"  Workspace: {WORKSPACE_URL}")
    print(f"  Repo path: {REPO_PATH}")
    print(f"  Catalog:   {CATALOG}")

    # Step 1: Connect repo
    repo_ok = ensure_repo(args.dry_run)
    if not repo_ok:
        print("\nWARNING: Repo connection failed — pipelines may fail to find library paths.")
        print("Continuing anyway...\n")

    # Step 2: Create / update pipelines
    pipeline_ids = deploy_pipelines(args.dry_run, args.all)

    # Step 3: Trigger
    trigger_pipelines(pipeline_ids, args.dry_run, args.all)

    print("\n=== Done ===")
    if not args.dry_run:
        print(f"\nMonitor pipelines at:")
        print(f"  {WORKSPACE_URL}/#joblist/pipelines")


if __name__ == "__main__":
    main()
