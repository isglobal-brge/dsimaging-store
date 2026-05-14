# dsimaging-store

Docker deployment for medical imaging storage used by `dsimaging-admin` and
the dsImaging DataSHIELD tooling.

## Quick start

The recommended operator flow is through `dsimaging-admin`:

```bash
pip install dsimaging-admin
dsimaging-admin store init ./store --store-source /path/to/dsimaging-store
dsimaging-admin store up ./store
dsimaging-admin store doctor ./store
```

If a controller image is available, use it instead of a local source build:

```bash
dsimaging-admin store init ./store \
  --controller-image davidsarratgonzalez/dsimaging-store:latest
dsimaging-admin store up ./store
```

The repository can still be run directly:

```bash
cp .env.example .env
docker compose up -d
```

## What it deploys

| Service | Purpose | Port |
|---|---|---|
| `minio` | S3-compatible object storage | 9000 API, 9001 console |
| `controller` | Webhook receiver and index reconciler | 8080 |
| `init` | Bucket creation, versioning and webhook setup | - |

## Controller API

The controller exposes:

- `GET /health` with bucket, source prefixes, dirty datasets and last errors.
- `GET /datasets` with S3 dataset status plus controller reconcile state.
- `POST /reconcile/<dataset-id>` to rebuild one dataset immediately.
- `POST /webhook/minio` for MinIO bucket notifications.

`dsimaging-admin doctor`, `list`, `status` and `reconcile` use these endpoints
when a controller URL is configured.

## Reconcile behaviour

The controller listens for MinIO events under:

- `datasets/<dataset_id>/source/images/`
- `datasets/<dataset_id>/source/masks/`

When source image or mask objects are created or removed, it rebuilds:

- `indexes/content_hash_index.parquet`
- `indexes/masks_content_hash_index.parquet` when masks exist
- `metadata/sample_manifests.parquet`
- `metadata/samples.parquet`
- `manifest.yaml`

`dsimaging-admin publish` writes `datasets/<id>/.publish-lock` while copying
from staging into the canonical source prefix. The controller defers reconcile
while that lock exists, so it does not write manifests for in-progress
publishes.

## Configuration

Copy `.env.example` to `.env` and edit:

| Variable | Default | Description |
|---|---|---|
| `MINIO_ROOT_USER` | `minioadmin` | MinIO root username |
| `MINIO_ROOT_PASSWORD` | `minioadmin123` | MinIO root password |
| `MINIO_PORT` | `9000` | MinIO API port |
| `MINIO_CONSOLE_PORT` | `9001` | MinIO web console port |
| `CONTROLLER_PORT` | `8080` | Controller port |
| `BUCKET_NAME` | `imaging-data` | Dataset bucket |
| `RECONCILE_INTERVAL_SECONDS` | `10` | Reconcile loop delay |
| `DSIMAGING_STORE_CONTROLLER_IMAGE` | `dsimaging-store-controller:local` | Image tag for local builds |

## Production notes

- Change default credentials.
- Enable TLS at the deployment boundary.
- Consider server-side encryption.
- Bucket versioning is enabled by the init service.
