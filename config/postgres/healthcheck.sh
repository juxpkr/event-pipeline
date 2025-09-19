#!/bin/sh
set -e

# -q 옵션은 성공 시 아무것도 출력하지 않고, 실패 시에만 에러를 출력.
pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q