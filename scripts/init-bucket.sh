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
WEBHOOK_RETRIES="${WEBHOOK_RETRIES:-12}"
WEBHOOK_RETRY_SECONDS="${WEBHOOK_RETRY_SECONDS:-5}"

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
mkdir -p /tmp/dsimaging-init
: > /tmp/dsimaging-init/.keep
mc cp /tmp/dsimaging-init/.keep "local/${BUCKET_NAME}/datasets/.keep" 2>/dev/null || true

configure_webhook() {
  attempt=1
  while [ "${attempt}" -le "${WEBHOOK_RETRIES}" ]; do
    if output=$(mc event add "local/${BUCKET_NAME}" arn:minio:sqs::DSIMAGING:webhook \
      --event put,delete \
      --prefix "datasets/" 2>&1); then
      echo "[init] Webhook notification configured"
      return 0
    fi

    if echo "${output}" | grep -qi "already"; then
      echo "[init] Webhook notification already configured"
      return 0
    fi

    echo "[init] Webhook notification attempt ${attempt}/${WEBHOOK_RETRIES} failed: ${output}"
    attempt=$((attempt + 1))
    if [ "${attempt}" -le "${WEBHOOK_RETRIES}" ]; then
      sleep "${WEBHOOK_RETRY_SECONDS}"
    fi
  done

  echo "[init] ERROR: Unable to configure webhook notification after ${WEBHOOK_RETRIES} attempts" >&2
  return 1
}

# Configure webhook notification for controller
configure_webhook

echo "[init] MinIO initialization complete"
echo "[init] Bucket: ${BUCKET_NAME}"
echo "[init] Upload datasets to: s3://${BUCKET_NAME}/datasets/<dataset_id>/source/images/"
