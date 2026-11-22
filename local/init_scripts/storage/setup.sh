#!/bin/sh
# Seed the object store (S3-compatible; the `mc` admin client works against it) with the
# shared demo bucket. Runs as a one-shot container (storage-init) after the store starts.
set -e

ENDPOINT="${STORAGE_ENDPOINT:-http://storage:9000}"
ACCESS_KEY="${STORAGE_ROOT_USER:-admin}"
SECRET_KEY="${STORAGE_ROOT_PASSWORD:-admin123}"

# Wait until the store answers, then register it as the 'local' alias. Each try is capped at
# 10 s: a connection made while the store is still starting can otherwise hang for good.
echo "Waiting for object storage at $ENDPOINT ..."
until timeout 10 mc alias set local "$ENDPOINT" "$ACCESS_KEY" "$SECRET_KEY" >/dev/null 2>&1; do
  sleep 2
done
echo "Object storage is ready."

# Buckets (idempotent).
mc mb --ignore-existing local/demo-bucket

# Learner access: home-api / JupyterHub create, per learner, the bucket <user>-lake with a
# hard quota and a storage policy named after the user (only that bucket; managers:
# everything) — RustFS doesn't expand ${jwt:…} in resources, so there's no shared policy.

echo "Buckets after setup:"
mc ls local/
echo "Object storage setup complete."
