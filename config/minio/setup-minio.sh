#!/bin/sh
mc alias set minio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
if mc ls minio/warehouse > /dev/null 2>&1; then
  echo ">>>> Bucket 'warehouse' already exists."
else
  echo ">>>> Creating bucket: warehouse"
  mc mb minio/warehouse
fi
exit 0