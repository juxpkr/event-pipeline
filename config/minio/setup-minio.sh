#!/bin/sh
mc alias set minio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
if mc ls minio/warehouse > /dev/null 2>&1; then
  echo ">>>> Bucket 'warehouse' already exists."
else
  echo ">>>> Creating bucket: warehouse"
  mc mb minio/warehouse
fi
exit 0


#!/bin/sh
# MinIO가 준비될 때까지 5초 간격으로 계속 시도
echo "Waiting for MinIO..."
until mc alias set minio http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD; do
  >&2 echo "MinIO is unavailable - sleeping"
  sleep 5
done

echo "MinIO is up - creating buckets..."
if mc ls minio/warehouse > /dev/null 2>&1; then
  echo ">>>> Bucket 'warehouse' already exists."
else
  echo ">>>> Creating bucket: warehouse"
  mc mb minio/warehouse
fi
echo "MinIO setup complete."