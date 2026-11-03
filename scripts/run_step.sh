#!/usr/bin/env bash
# 运行单个 Spark 作业：bash scripts/run_step.sh <脚本名> <dt>
# 本地：直接 python 运行（local[*]）；集群：SPARK_ON_YARN=1 时用 spark-submit --master yarn 提交
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STEP="$1"; DT="$2"
REPORT_DATE="${REPORT_DATE:-$DT}"
LOG="${LOG:-$ROOT/logs/pipeline_${DT}.log}"
mkdir -p "$(dirname "$LOG")"
export PYTHONPATH="$ROOT/spark_jobs:${PYTHONPATH:-}"
echo ">>> $(date '+%T') $STEP dt=$DT" | tee -a "$LOG"
if [ -n "${SPARK_ON_YARN:-}" ]; then
  (cd "$ROOT/spark_jobs" && zip -qr /tmp/rental_dq_lib.zip lib common.py)
  spark-submit --master yarn --deploy-mode client \
    --py-files /tmp/rental_dq_lib.zip \
    --conf spark.yarn.appMasterEnv.PYTHONPATH=. \
    "$ROOT/spark_jobs/$STEP" --dt "$DT" --report-date "$REPORT_DATE" 2>&1 | tee -a "$LOG"
else
  python "$ROOT/spark_jobs/$STEP" --dt "$DT" --report-date "$REPORT_DATE" 2>&1 | tee -a "$LOG"
fi
exit "${PIPESTATUS[0]}"
