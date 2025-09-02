#!/bin/sh
mc alias set minio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
if ! mc ls minio | grep -q 'warehouse'; then
  echo ">>>> Creating bucket: warehouse"
  mc mb minio/warehouse
else
  echo ">>>> Bucket 'warehouse' already exists."
fi
exit 0