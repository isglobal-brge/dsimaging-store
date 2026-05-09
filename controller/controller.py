#!/usr/bin/env python3
"""dsImagingStore controller.

Receives MinIO bucket notifications and reconciles dataset artifacts:
content_hash_index.parquet, mask hash indexes, sample_manifests.parquet,
samples.parquet and manifest.yaml. Direct uploads to
datasets/<id>/source/images/ and datasets/<id>/source/masks/ therefore
converge to the same layout produced by dsimaging-admin publish/rescan.
"""

import hashlib
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("controller")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
BUCKET = os.environ.get("BUCKET_NAME", "imaging-data")
RECONCILE_INTERVAL_SECONDS = int(os.environ.get("RECONCILE_INTERVAL_SECONDS", "10"))

IMAGE_EXTENSIONS = frozenset({
    ".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm",
    ".svs", ".tif", ".tiff", ".png", ".jpg",
})

state_lock = threading.Lock()
dirty_datasets = set()
last_reconcile = {}
last_errors = {}


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
            except Exception as e:
                record_failure(dataset_id, e)
                log.exception("Reconcile failed for dataset %s: %s", dataset_id, e)
        time.sleep(RECONCILE_INTERVAL_SECONDS)


def reconcile_dataset(dataset_id):
    s3 = get_s3()
    prefix = f"datasets/{dataset_id}"
    objects = list_objects(s3, f"{prefix}/source/images/")
    mask_objects = list_objects(s3, f"{prefix}/source/masks/")
    samples = scan_s3_images(s3, prefix, objects)
    masks = scan_s3_masks(
        s3, prefix, mask_objects,
        sample_ids=[sample["sample_id"] for sample in samples],
    )
    if not samples:
        if dataset_artifacts_exist(s3, prefix):
            write_dataset_artifacts(s3, prefix, dataset_id, samples, masks)
        return 0, len(masks)
    write_dataset_artifacts(s3, prefix, dataset_id, samples, masks)
    return len(samples), len(masks)


def dataset_artifacts_exist(s3, prefix):
    return any(
        object_exists(s3, f"{prefix}/{suffix}")
        for suffix in (
            "manifest.yaml",
            "indexes/content_hash_index.parquet",
            "metadata/sample_manifests.parquet",
            "metadata/samples.parquet",
        )
    )


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
    root = f"{prefix.rstrip('/')}/source/images/"
    single_files = []
    dicom_groups = {}

    for obj in objects:
        key = obj["key"]
        if not key.startswith(root):
            continue
        rel = key[len(root):]
        if not rel or rel.endswith("/"):
            continue
        filename = rel.rsplit("/", 1)[-1]
        if not is_image_file(filename):
            continue
        if "/" in rel and filename.lower().endswith(".dcm"):
            sample_id = rel.split("/", 1)[0]
            dicom_groups.setdefault(sample_id, []).append((rel, obj))
        else:
            single_files.append((rel, obj))

    samples = []
    for rel, obj in sorted(single_files, key=lambda item: item[0]):
        filename = rel.rsplit("/", 1)[-1]
        samples.append({
            "sample_id": sample_id_from_filename(filename),
            "source_kind": "single_file",
            "primary_filename": filename,
            "uri_path": rel,
            "files": [{"path": rel, "role": "primary"}],
            "content_hash": sha256_s3_object(s3, obj["key"]),
            "size": int(obj.get("size", 0)),
            "last_modified": obj.get("last_modified"),
            "etag": obj.get("etag"),
        })

    for sample_id in sorted(dicom_groups):
        h = hashlib.sha256()
        total_size = 0
        files = []
        last_modified = None
        etags = []
        for rel, obj in sorted(dicom_groups[sample_id], key=lambda item: item[0]):
            content_hash = sha256_s3_object(s3, obj["key"])
            h.update(content_hash.encode())
            total_size += int(obj.get("size", 0))
            last_modified = obj.get("last_modified") or last_modified
            if obj.get("etag"):
                etags.append(obj["etag"])
            files.append({"path": rel, "role": "slice"})
        samples.append({
            "sample_id": sample_id,
            "source_kind": "dicom_series",
            "primary_filename": None,
            "uri_path": f"{sample_id}/",
            "files": files,
            "content_hash": h.hexdigest(),
            "size": total_size,
            "last_modified": last_modified,
            "etag": ",".join(etags) if etags else None,
        })

    return sorted(samples, key=lambda sample: sample["sample_id"])


def scan_s3_masks(s3, prefix, objects, sample_ids=None):
    root = f"{prefix.rstrip('/')}/source/masks/"
    masks = []

    for obj in objects:
        key = obj["key"]
        if not key.startswith(root):
            continue
        rel = key[len(root):]
        if not rel or rel.endswith("/"):
            continue
        filename = rel.rsplit("/", 1)[-1]
        if not is_image_file(filename):
            continue
        masks.append({
            "sample_id": sample_id_from_mask_filename(filename, sample_ids),
            "source_kind": "mask_file",
            "primary_filename": filename,
            "uri_path": rel,
            "files": [{"path": rel, "role": "mask"}],
            "content_hash": sha256_s3_object(s3, key),
            "size": int(obj.get("size", 0)),
            "last_modified": obj.get("last_modified"),
            "etag": obj.get("etag"),
        })

    return sorted(masks, key=lambda sample: sample["sample_id"])


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


def build_hash_index(prefix, samples, source_path="images"):
    now = utc_now()
    if not samples:
        return pa.table({
            "sample_id": pa.array([], type=pa.string()),
            "uri": pa.array([], type=pa.string()),
            "content_hash": pa.array([], type=pa.string()),
            "size": pa.array([], type=pa.int64()),
            "last_modified": pa.array([], type=pa.string()),
            "version_id": pa.array([], type=pa.string()),
            "etag": pa.array([], type=pa.string()),
            "source_kind": pa.array([], type=pa.string()),
        })
    return pa.table({
        "sample_id": [s["sample_id"] for s in samples],
        "uri": [f"s3://{BUCKET}/{prefix}/source/{source_path}/{s['uri_path']}" for s in samples],
        "content_hash": [s["content_hash"] for s in samples],
        "size": pa.array([s["size"] for s in samples], type=pa.int64()),
        "last_modified": [s.get("last_modified") or now for s in samples],
        "version_id": pa.array([None for _ in samples], type=pa.string()),
        "etag": pa.array([s.get("etag") for s in samples], type=pa.string()),
        "source_kind": [s["source_kind"] for s in samples],
    })


def build_sample_manifests(samples):
    if not samples:
        return pa.table({
            "sample_id": pa.array([], type=pa.string()),
            "source_kind": pa.array([], type=pa.string()),
            "primary_uri": pa.array([], type=pa.string()),
            "files_json": pa.array([], type=pa.string()),
            "content_hash": pa.array([], type=pa.string()),
            "n_files": pa.array([], type=pa.int32()),
        })
    return pa.table({
        "sample_id": [s["sample_id"] for s in samples],
        "source_kind": [s["source_kind"] for s in samples],
        "primary_uri": pa.array([s["primary_filename"] for s in samples], type=pa.string()),
        "files_json": [json.dumps(s["files"]) for s in samples],
        "content_hash": [s["content_hash"] for s in samples],
        "n_files": pa.array([len(s["files"]) for s in samples], type=pa.int32()),
    })


def build_samples_metadata(samples, extra_metadata=None):
    if not samples:
        base = pa.table({
            "sample_id": pa.array([], type=pa.string()),
            "source_kind": pa.array([], type=pa.string()),
            "n_files": pa.array([], type=pa.int32()),
        })
    else:
        base = pa.table({
            "sample_id": [s["sample_id"] for s in samples],
            "source_kind": [s["source_kind"] for s in samples],
            "n_files": pa.array([len(s["files"]) for s in samples], type=pa.int32()),
        })
    if extra_metadata is None:
        return base
    return left_join_metadata(base, extra_metadata)


def normalise_metadata_table(table):
    if "sample_id" not in table.column_names:
        return None
    idx = table.column_names.index("sample_id")
    return table.set_column(idx, "sample_id", table["sample_id"].cast(pa.string()))


def left_join_metadata(base, extra_metadata):
    extra_metadata = normalise_metadata_table(extra_metadata)
    if extra_metadata is None:
        return base

    base_ids = base["sample_id"].to_pylist()
    rows_by_id = {}
    for row in extra_metadata.to_pylist():
        sample_id = row.get("sample_id")
        if sample_id in rows_by_id:
            log.warning("Ignoring preserved metadata with duplicate sample_id: %s", sample_id)
            return base
        rows_by_id[sample_id] = row

    extra_columns = [
        name for name in extra_metadata.column_names
        if name != "sample_id" and name not in base.column_names
    ]
    arrays = {name: base[name] for name in base.column_names}
    schema = extra_metadata.schema
    for name in extra_columns:
        field = schema.field(name)
        values = [
            rows_by_id.get(sample_id, {}).get(name)
            for sample_id in base_ids
        ]
        arrays[name] = pa.array(values, type=field.type)
    return pa.table(arrays)


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
    manifest = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "modality": modality,
        "assets": {
            "images": {
                "uri": f"s3://{BUCKET}/{prefix}/source/images/",
                "kind": "image_root",
            },
        },
        "metadata": {
            "uri": f"s3://{BUCKET}/{prefix}/metadata/samples.parquet",
            "format": "parquet",
        },
        "content_hash_index": {
            "uri": f"s3://{BUCKET}/{prefix}/indexes/content_hash_index.parquet",
            "format": "parquet",
        },
        "sample_manifests": {
            "uri": f"s3://{BUCKET}/{prefix}/metadata/sample_manifests.parquet",
            "format": "parquet",
        },
    }
    if has_masks:
        manifest["assets"]["masks"] = {
            "uri": f"s3://{BUCKET}/{prefix}/source/masks/",
            "kind": "mask_root",
            "content_hash_index": (
                f"s3://{BUCKET}/{prefix}/indexes/"
                "masks_content_hash_index.parquet"
            ),
        }
    return manifest


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


def sha256_s3_object(s3, key):
    h = hashlib.sha256()
    response = s3.get_object(Bucket=BUCKET, Key=key)
    body = response["Body"]
    try:
        for chunk in iter(lambda: body.read(65536), b""):
            if chunk:
                h.update(chunk)
    finally:
        body.close()
    return h.hexdigest()


def is_image_file(filename):
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in IMAGE_EXTENSIONS)


def sample_id_from_filename(filename):
    lower = filename.lower()
    for ext in sorted(IMAGE_EXTENSIONS, key=len, reverse=True):
        if lower.endswith(ext):
            return filename[:-len(ext)]
    return os.path.splitext(filename)[0]


def sample_id_from_mask_filename(filename, sample_ids=None):
    stem = sample_id_from_filename(filename)
    if sample_ids:
        for sample_id in sorted(sample_ids, key=len, reverse=True):
            if stem == sample_id or stem.startswith(f"{sample_id}_") or stem.startswith(f"{sample_id}-"):
                return sample_id
    suffix_pattern = (
        r"(?i)(?:[_-](?:mask|seg|label|labels|roi|gtv[-_]?\d*|"
        r"lesion[-_]?\d*|tumou?r[-_]?\d*))$"
    )
    stripped = re.sub(suffix_pattern, "", stem)
    return stripped or stem


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
