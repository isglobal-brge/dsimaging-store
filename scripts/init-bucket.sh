#!/bin/sh
# One-time MinIO initialization:
# 1. Create bucket
# 2. Enable versioning
# 3. Configure webhook notifications
set -e

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin123}"
BUCKET_NAME="${BUCKET_NAME:-imaging-data}"

echo "[init] Configuring MinIO at ${MINIO_ENDPOINT}"

# Configure mc alias
mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" --api s3v4

# Create bucket if not exists
if mc ls "local/${BUCKET_NAME}" >/dev/null 2>&1; then
  echo "[init] Bucket '${BUCKET_NAME}' already exists"
else
  mc mb "local/${BUCKET_NAME}"
  echo "[init] Bucket '${BUCKET_NAME}' created"
fi

# Enable versioning
mc version enable "local/${BUCKET_NAME}"
echo "[init] Versioning enabled on '${BUCKET_NAME}'"

# Create datasets/ prefix structure
mc cp /dev/null "local/${BUCKET_NAME}/datasets/.keep" 2>/dev/null || true

# Configure webhook notification for controller
mc event add "local/${BUCKET_NAME}" arn:minio:sqs::DSIMAGING:webhook \
  --event put,delete \
  --prefix "datasets/" \
  2>/dev/null || echo "[init] Webhook notification already configured or controller not ready"

echo "[init] MinIO initialization complete"
echo "[init] Bucket: ${BUCKET_NAME}"
echo "[init] Upload datasets to: s3://${BUCKET_NAME}/datasets/<dataset_id>/source/images/"
