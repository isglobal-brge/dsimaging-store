# dsimaging-store

Docker deployment for medical imaging storage (MinIO/S3) for DataSHIELD.

## Quick start

```bash
# 1. Clone and configure
git clone https://github.com/davidsarratgonzalez/dsimaging-store.git
cd dsimaging-store
cp .env.example .env
# Edit .env with your credentials

# 2. Start
docker compose up -d

# 3. Publish datasets using dsimaging-admin
pip install dsimaging-admin
dsimaging-admin publish --dataset-id lung_ct_v1 --source /data/lung_ct --modality ct
```

## What it deploys

| Service | Purpose | Port |
|---|---|---|
| **minio** | S3-compatible object storage | 9000 (API), 9001 (console) |
| **controller** | Webhook receiver for bucket notifications | 8080 |
| **init** | One-time setup (bucket creation, versioning) | - |

## Architecture

```
Hospital admin                     dsimaging-store (Docker)
  |                                  |
  | dsimaging-admin publish          | MinIO (S3-compatible)
  |------------------------------>   |   s3://imaging-data/datasets/...
  |                                  |
  | Register resource in Opal        | Controller (webhook)
  |                                  |   auto-detects changes
  |                                  |
DataSHIELD R session                 |
  |                                  |
  | imaging+dataset://lung_ct_v1     |
  |------------------------------>   |
  | resolve -> manifest -> images    |
```

## Configuration

Copy `.env.example` to `.env` and edit:

| Variable | Default | Description |
|---|---|---|
| `MINIO_ROOT_USER` | `minioadmin` | MinIO root username |
| `MINIO_ROOT_PASSWORD` | `minioadmin123` | MinIO root password |
| `MINIO_PORT` | `9000` | MinIO API port |
| `MINIO_CONSOLE_PORT` | `9001` | MinIO web console port |
| `BUCKET_NAME` | `imaging-data` | Bucket for imaging datasets |

## Production notes

- Change the default credentials in `.env`
- Enable TLS (see MinIO docs)
- Consider enabling server-side encryption (SSE)
- Bucket versioning is enabled automatically by the init service
