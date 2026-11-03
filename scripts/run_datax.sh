#!/usr/bin/env bash
# 生产环境：DataX 把 MySQL 业务库同步到 HDFS，再给 Hive ODS 表加分区
# 用法：bash scripts/run_datax.sh <dt>
# 依赖环境变量：DATAX_HOME MYSQL_HOST MYSQL_USER MYSQL_PASSWORD NAMENODE
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DT="${1:?dt}"
for job in "$ROOT"/datax/jobs/mysql2hdfs_*.json; do
  tbl="$(basename "$job" .json)"; tbl="${tbl#mysql2hdfs_}"
  hdfs dfs -mkdir -p "/user/hive/warehouse/ods.db/${tbl}/dt=${DT}"
  python "$DATAX_HOME/bin/datax.py" \
    -p "-Ddt=${DT} -Dmysql_host=${MYSQL_HOST} -Dmysql_user=${MYSQL_USER} -Dmysql_password=${MYSQL_PASSWORD} -Dnamenode=${NAMENODE}" \
    "$job"
  hive -e "ALTER TABLE ods.${tbl} ADD IF NOT EXISTS PARTITION (dt='${DT}');"
done
