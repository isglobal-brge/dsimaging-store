#!/usr/bin/env python3
"""dsImagingStore controller.

Receives MinIO bucket notifications and reconciles dataset artifacts:
content_hash_index.parquet, mask hash indexes, sample_manifests.parquet,
samples.parquet and manifest.yaml. Direct uploads to
datasets/<id>/source/images/ and datasets/<id>/source/masks/ therefore
converge to the same layout produced by dsimaging-admin publish/rescan.
"""

import json
import logging
import os
import re
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import unquote_plus

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from dsimaging_admin.manifest import (
    build_hash_index as core_build_hash_index,
    build_sample_manifests as core_build_sample_manifests,
    build_samples_metadata as core_build_samples_metadata,
    generate_manifest as core_generate_manifest,
    scan_s3_images as core_scan_s3_images,
    scan_s3_masks as core_scan_s3_masks,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("controller")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
BUCKET = os.environ.get("BUCKET_NAME", "imaging-data")
RECONCILE_INTERVAL_SECONDS = int(os.environ.get("RECONCILE_INTERVAL_SECONDS", "10"))
PUBLISH_LOCK = ".publish-lock"
WEBHOOK_PREFIX = "datasets/"
SOURCE_PREFIXES = [
    "datasets/<dataset_id>/source/images/",
    "datasets/<dataset_id>/source/masks/",
]
MANAGED_ARTIFACTS = (
    "manifest.yaml",
    "indexes/content_hash_index.parquet",
    "indexes/masks_content_hash_index.parquet",
    "metadata/sample_manifests.parquet",
    "metadata/samples.parquet",
)
DATASET_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")

state_lock = threading.Lock()
dirty_datasets = set()
last_reconcile = {}
last_errors = {}


class PublishInProgress(Exception):
    """Raised when a dataset has an active publish lock."""


def get_s3():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def extract_dataset_id_from_source_key(key):
    """Extract dataset_id from datasets/<id>/source/{images,masks}/... keys."""
    parts = key.split("/")
    if (
        len(parts) >= 5
        and parts[0] == "datasets"
        and parts[2] == "source"
        and parts[3] in {"images", "masks"}
    ):
        return parts[1]
    return None


def mark_dirty(dataset_id):
    with state_lock:
        dirty_datasets.add(dataset_id)


def pop_dirty_batch():
    with state_lock:
        batch = sorted(dirty_datasets)
        dirty_datasets.clear()
    return batch


def record_success(dataset_id, n_samples, n_masks=0):
    with state_lock:
        last_reconcile[dataset_id] = {
            "at": utc_now(),
            "samples": n_samples,
            "masks": n_masks,
        }
        last_errors.pop(dataset_id, None)


def record_failure(dataset_id, error):
    with state_lock:
        dirty_datasets.add(dataset_id)
        last_errors[dataset_id] = {
            "at": utc_now(),
            "error": str(error),
        }


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path.startswith("/reconcile/"):
            dataset_id = self.path.split("/reconcile/", 1)[1]
            if not DATASET_ID_RE.match(dataset_id or ""):
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "invalid dataset_id"}).encode())
                return
            try:
                n_samples, n_masks = reconcile_dataset(dataset_id)
                record_success(dataset_id, n_samples, n_masks)
                self.write_json({
                    "status": "ok",
                    "dataset_id": dataset_id,
                    "samples": n_samples,
                    "masks": n_masks,
                })
            except PublishInProgress:
                mark_dirty(dataset_id)
                self.send_response(409)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "locked",
                    "dataset_id": dataset_id,
                    "error": "publish in progress",
                }).encode())
            except Exception as e:
                record_failure(dataset_id, e)
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
            return

        if self.path != "/webhook/minio":
            self.send_response(404)
            self.end_headers()
            return

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        try:
            events = json.loads(body)
            records = events.get("Records", [])
            for record in records:
                raw_key = record.get("s3", {}).get("object", {}).get("key", "")
                key = unquote_plus(raw_key)
                event_name = record.get("eventName", "")
                dataset_id = extract_dataset_id_from_source_key(key)
                if dataset_id:
                    log.info("Event: %s on %s (dataset: %s)", event_name, key, dataset_id)
                    mark_dirty(dataset_id)
        except Exception as e:
            log.exception("Webhook parse error: %s", e)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def do_GET(self):
        if self.path == "/health":
            self.write_json({
                "status": "ok",
                "bucket": BUCKET,
                "minio_endpoint": MINIO_ENDPOINT,
                "webhook_prefix": WEBHOOK_PREFIX,
                "source_prefixes": SOURCE_PREFIXES,
                "reconcile_interval_seconds": RECONCILE_INTERVAL_SECONDS,
                "dirty_datasets": sorted_snapshot(dirty_datasets),
                "last_reconcile": snapshot(last_reconcile),
                "last_errors": snapshot(last_errors),
            })
            return

        if self.path == "/datasets":
            try:
                self.write_json({"datasets": list_datasets()})
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
            return

        self.send_response(404)
        self.end_headers()

    def write_json(self, payload):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload, sort_keys=True).encode())

    def log_message(self, fmt, *args):
        pass


def list_datasets():
    s3 = get_s3()
    datasets = []
    paginator = s3.get_paginator("list_objects_v2")
    with state_lock:
        dirty = set(dirty_datasets)
        reconcile_snapshot = dict(last_reconcile)
        error_snapshot = dict(last_errors)

    for page in paginator.paginate(Bucket=BUCKET, Prefix="datasets/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            dataset_id = cp["Prefix"].strip("/").split("/")[-1]
            if not prefix_has_current_objects(s3, f"datasets/{dataset_id}/"):
                continue
            has_manifest = object_exists(s3, f"datasets/{dataset_id}/manifest.yaml")
            datasets.append({
                "dataset_id": dataset_id,
                "status": "published" if has_manifest else "incomplete",
                "dirty": dataset_id in dirty,
                "last_reconcile": reconcile_snapshot.get(dataset_id),
                "last_error": error_snapshot.get(dataset_id),
            })
    return datasets


def object_exists(s3, key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def prefix_has_current_objects(s3, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if object_exists(s3, obj["Key"]):
                return True
    return False


def reconcile_loop():
    while True:
        batch = pop_dirty_batch()
        for dataset_id in batch:
            try:
                n_samples, n_masks = reconcile_dataset(dataset_id)
                record_success(dataset_id, n_samples, n_masks)
                log.info(
                    "Reconciled dataset %s (%s samples, %s masks)",
                    dataset_id, n_samples, n_masks,
                )
            except PublishInProgress:
                mark_dirty(dataset_id)
                log.info("Reconcile deferred for dataset %s: publish in progress", dataset_id)
            except Exception as e:
                record_failure(dataset_id, e)
                log.exception("Reconcile failed for dataset %s: %s", dataset_id, e)
        time.sleep(RECONCILE_INTERVAL_SECONDS)


def reconcile_dataset(dataset_id):
    s3 = get_s3()
    prefix = f"datasets/{dataset_id}"
    if object_exists(s3, f"{prefix}/{PUBLISH_LOCK}"):
        raise PublishInProgress()
    objects = list_objects(s3, f"{prefix}/source/images/")
    mask_objects = list_objects(s3, f"{prefix}/source/masks/")
    samples = scan_s3_images(s3, prefix, objects)
    masks = scan_s3_masks(
        s3, prefix, mask_objects,
        sample_ids=[sample["sample_id"] for sample in samples],
    )
    if not samples:
        deleted = delete_dataset_artifacts(s3, prefix)
        if deleted:
            log.info(
                "Removed %s managed artifact(s) for dataset %s because no source images remain",
                deleted,
                dataset_id,
            )
        return 0, len(masks)
    write_dataset_artifacts(s3, prefix, dataset_id, samples, masks)
    return len(samples), len(masks)


def delete_dataset_artifacts(s3, prefix):
    keys = [
        f"{prefix}/{suffix}"
        for suffix in MANAGED_ARTIFACTS
        if object_exists(s3, f"{prefix}/{suffix}")
    ]
    return delete_current_keys(s3, keys)


def delete_current_keys(s3, keys):
    deleted = 0
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        if not chunk:
            continue
        response = s3.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )
        deleted += len(chunk) - len(response.get("Errors", []))
    return deleted


def list_objects(s3, prefix):
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append({
                "key": obj["Key"],
                "size": int(obj["Size"]),
                "last_modified": obj["LastModified"].isoformat(),
                "etag": obj.get("ETag", "").strip('"') or None,
            })
    return objects


def scan_s3_images(s3, prefix, objects):
    return core_scan_s3_images(s3, BUCKET, prefix, objects)


def scan_s3_masks(s3, prefix, objects, sample_ids=None):
    return core_scan_s3_masks(s3, BUCKET, prefix, objects, sample_ids=sample_ids)


def write_dataset_artifacts(s3, prefix, dataset_id, samples, masks=None):
    modality = existing_modality(s3, prefix, fallback="unknown")
    extra_metadata = read_existing_samples_metadata(s3, prefix)
    masks = masks or []
    with tempfile.TemporaryDirectory() as tmpdir:
        uploads = [
            ("indexes/content_hash_index.parquet",
             write_parquet(tmpdir, "content_hash_index.parquet",
                           build_hash_index(prefix, samples, source_path="images"))),
            ("metadata/sample_manifests.parquet",
             write_parquet(tmpdir, "sample_manifests.parquet",
                           build_sample_manifests(samples))),
            ("metadata/samples.parquet",
             write_parquet(tmpdir, "samples.parquet",
                           build_samples_metadata(samples, extra_metadata))),
            ("manifest.yaml",
             write_yaml(tmpdir, "manifest.yaml",
                        generate_manifest(dataset_id, prefix, modality,
                                          has_masks=bool(masks)))),
        ]
        if masks:
            uploads.append(
                ("indexes/masks_content_hash_index.parquet",
                 write_parquet(tmpdir, "masks_content_hash_index.parquet",
                               build_hash_index(prefix, masks, source_path="masks")))
            )
        for rel_key, path in uploads:
            s3.upload_file(path, BUCKET, f"{prefix}/{rel_key}")
        if not masks:
            mask_index_key = f"{prefix}/indexes/masks_content_hash_index.parquet"
            if object_exists(s3, mask_index_key):
                delete_current_keys(s3, [mask_index_key])


def build_hash_index(prefix, samples, source_path="images"):
    return core_build_hash_index(samples, BUCKET, prefix, source_path=source_path)


def build_sample_manifests(samples):
    return core_build_sample_manifests(samples)


def build_samples_metadata(samples, extra_metadata=None):
    try:
        return core_build_samples_metadata(samples, extra_metadata=extra_metadata)
    except ValueError as e:
        log.warning("Ignoring preserved metadata during reconcile: %s", e)
        return core_build_samples_metadata(samples)


def read_existing_samples_metadata(s3, prefix):
    try:
        response = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/metadata/samples.parquet")
        body = response["Body"]
        try:
            data = body.read()
        finally:
            body.close()
        if not data:
            return None
        return pq.read_table(pa.BufferReader(data))
    except Exception:
        return None


def generate_manifest(dataset_id, prefix, modality, has_masks=False):
    return core_generate_manifest(
        dataset_id, BUCKET, prefix, modality=modality, has_masks=has_masks
    )


def existing_modality(s3, prefix, fallback):
    try:
        response = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/manifest.yaml")
        body = response["Body"]
        try:
            manifest = yaml.safe_load(body.read()) or {}
        finally:
            body.close()
        return manifest.get("modality") or fallback
    except Exception:
        return fallback


def write_parquet(tmpdir, filename, table):
    path = os.path.join(tmpdir, filename)
    pq.write_table(table, path)
    return path


def write_yaml(tmpdir, filename, payload):
    path = os.path.join(tmpdir, filename)
    with open(path, "w") as f:
        yaml.dump(payload, f, default_flow_style=False, sort_keys=False)
    return path


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def snapshot(mapping):
    with state_lock:
        return dict(mapping)


def sorted_snapshot(values):
    with state_lock:
        return sorted(values)


def main():
    port = int(os.environ.get("PORT", "8080"))
    thread = threading.Thread(target=reconcile_loop, daemon=True)
    thread.start()
    server = HTTPServer(("0.0.0.0", port), Handler)
    log.info("Controller listening on port %s", port)
    log.info("MinIO: %s, Bucket: %s", MINIO_ENDPOINT, BUCKET)
    log.info("Reconcile interval: %ss", RECONCILE_INTERVAL_SECONDS)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")
        server.server_close()


if __name__ == "__main__":
    main()
