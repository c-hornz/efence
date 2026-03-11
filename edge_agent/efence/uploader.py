"""
E-Fence Edge Agent — Databricks Files API Uploader
=====================================================
Flushes buffered records to Unity Catalog Volumes using the
Databricks Files API (REST PUT).

Endpoint:
  PUT https://{workspace}/api/2.0/fs/files/Volumes/{catalog}/{schema}/{volume}/{filename}

Auth:
  Authorization: Bearer {EFENCE_DATABRICKS_TOKEN}

File format:
  NDJSON — one JSON record per line, matching the bronze schema exactly.

Retry strategy:
  Up to max_retries attempts with exponential backoff (2s, 4s, 8s).
  On final failure, records remain in the buffer (uploaded_at stays NULL)
  and will be retried on the next upload cycle.
"""

import io
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import List, Tuple

import requests

log = logging.getLogger("efence.uploader")


class Uploader:
    def __init__(self, cfg: dict):
        db_cfg = cfg["databricks"]
        self._workspace  = db_cfg["workspace_url"].rstrip("/")
        self._token      = db_cfg["token"]
        self._wifi_path  = db_cfg["wifi_volume_path"].rstrip("/")
        self._bt_path    = db_cfg["bt_volume_path"].rstrip("/")
        self._timeout    = db_cfg.get("upload_timeout_seconds", 30)
        self._max_retry  = db_cfg.get("max_retries", 3)
        self._sensor_id  = cfg["sensor"]["id"].replace("/", "-")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upload_wifi(self, records: List[Tuple[int, dict]]) -> List[int]:
        """
        Upload Wi-Fi records to the Databricks landing volume.
        Returns list of row_ids that were successfully uploaded.
        """
        return self._upload("wifi", self._wifi_path, records)

    def upload_bt(self, records: List[Tuple[int, dict]]) -> List[int]:
        """
        Upload Bluetooth records to the Databricks landing volume.
        Returns list of row_ids that were successfully uploaded.
        """
        return self._upload("bt", self._bt_path, records)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _upload(
        self,
        protocol: str,
        volume_path: str,
        records: List[Tuple[int, dict]],
    ) -> List[int]:
        if not records:
            return []

        row_ids      = [r[0] for r in records]
        record_dicts = [r[1] for r in records]

        filename = self._make_filename(protocol)
        api_path = f"{volume_path}/{filename}"
        url      = f"{self._workspace}/api/2.0/fs/files{api_path}"

        body = self._to_ndjson(record_dicts)

        success = self._put_with_retry(url, body, filename)
        if success:
            log.info(
                "Uploaded %d %s records → %s",
                len(records), protocol, filename,
            )
            return row_ids
        else:
            log.warning(
                "Upload failed for %d %s records — will retry next cycle",
                len(records), protocol,
            )
            return []

    def _put_with_retry(self, url: str, body: bytes, filename: str) -> bool:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type":  "application/octet-stream",
        }
        for attempt in range(1, self._max_retry + 1):
            try:
                resp = requests.put(
                    url,
                    data=io.BytesIO(body),
                    headers=headers,
                    timeout=self._timeout,
                )
                if resp.status_code in (200, 201, 204):
                    return True

                log.warning(
                    "PUT %s → HTTP %d: %s (attempt %d/%d)",
                    filename, resp.status_code,
                    resp.text[:200], attempt, self._max_retry,
                )

                # 401/403: token problem — no point retrying
                if resp.status_code in (401, 403):
                    log.error("Authentication error — check EFENCE_DATABRICKS_TOKEN")
                    return False

            except requests.exceptions.ConnectionError:
                log.warning("No network connectivity (attempt %d/%d)", attempt, self._max_retry)
            except requests.exceptions.Timeout:
                log.warning("Upload timed out after %ds (attempt %d/%d)",
                            self._timeout, attempt, self._max_retry)
            except Exception as exc:
                log.error("Unexpected upload error: %s", exc)
                return False

            if attempt < self._max_retry:
                backoff = 2 ** attempt
                log.info("Retrying in %ds...", backoff)
                time.sleep(backoff)

        return False

    def _make_filename(self, protocol: str) -> str:
        """Generate a unique, sortable filename for the batch."""
        ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        return f"{protocol}_{self._sensor_id}_{ts}_{uid}.json"

    @staticmethod
    def _to_ndjson(records: List[dict]) -> bytes:
        """Serialise records as newline-delimited JSON bytes."""
        lines = [json.dumps(r, separators=(",", ":")) + "\n" for r in records]
        return "".join(lines).encode("utf-8")
