#!/bin/sh
set -e

echo "Waiting for MinIO to start..."
sleep 5

mc alias set local http://minio:9010 "$MINIO_USER" "$MINIO_PASSWORD"

mc mb --ignore-existing local/bronze
mc mb --ignore-existing local/silver
mc mb --ignore-existing local/gold

echo "Buckets created!"