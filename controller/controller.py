#!/usr/bin/env python3
"""dsImagingStore Controller

Receives MinIO bucket notifications via webhook and incrementally
updates dataset indexes (content_hash_index, sample_manifests).

Endpoints:
    POST /webhook/minio  - MinIO bucket notification
    GET  /health         - Health check
    GET  /datasets       - List datasets with status
"""

import hashlib
import json
import logging
import os
import tempfile
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("controller")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
BUCKET = os.environ.get("BUCKET_NAME", "imaging-data")

# Track dirty datasets (set by webhook, cleared by reconcile)
dirty_datasets = set()


def get_s3():
    import boto3
    return boto3.client("s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="")


def extract_dataset_id(key):
    """Extract dataset_id from an S3 key like datasets/lung_ct_v1/source/images/..."""
    parts = key.split("/")
    if len(parts) >= 2 and parts[0] == "datasets":
        return parts[1]
    return None


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/webhook/minio":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            try:
                events = json.loads(body)
                records = events.get("Records", [])
                for record in records:
                    key = record.get("s3", {}).get("object", {}).get("key", "")
                    event_name = record.get("eventName", "")
                    ds_id = extract_dataset_id(key)
                    if ds_id:
                        log.info(f"Event: {event_name} on {key} (dataset: {ds_id})")
                        dirty_datasets.add(ds_id)
            except Exception as e:
                log.error(f"Webhook parse error: {e}")

            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "ok",
                "dirty_datasets": list(dirty_datasets),
                "bucket": BUCKET,
            }).encode())
        elif self.path == "/datasets":
            try:
                s3 = get_s3()
                datasets = []
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=BUCKET, Prefix="datasets/", Delimiter="/"):
                    for cp in page.get("CommonPrefixes", []):
                        ds_id = cp["Prefix"].strip("/").split("/")[-1]
                        has_manifest = False
                        try:
                            s3.head_object(Bucket=BUCKET, Key=f"datasets/{ds_id}/manifest.yaml")
                            has_manifest = True
                        except Exception:
                            pass
                        datasets.append({
                            "dataset_id": ds_id,
                            "status": "published" if has_manifest else "incomplete",
                            "dirty": ds_id in dirty_datasets,
                        })
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"datasets": datasets}).encode())
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress default logging


def main():
    port = int(os.environ.get("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    log.info(f"Controller listening on port {port}")
    log.info(f"MinIO: {MINIO_ENDPOINT}, Bucket: {BUCKET}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")
        server.server_close()


if __name__ == "__main__":
    main()
