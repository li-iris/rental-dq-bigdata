#!/usr/bin/env bash
# 本地一键跑通：生成合成数据 → ODS → DWD → DIM/DWS → 质量检查 → 导出 → 质量门禁
# 用法：bash scripts/run_pipeline.sh 2026-11-02            （只有 warn，退出码 0）
#       bash scripts/run_pipeline.sh 2026-11-02 --with-errors（注入 error，退出码 2）
# 退出码：0 正常；1 某一步失败；2 数据存在 error 级问题
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DT="${1:?请传入业务日期，如 2026-11-02}"
EXTRA="${2:-}"
export REPORT_DATE="${REPORT_DATE:-$DT}"

LOCK="$ROOT/.pipeline.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  echo "已有任务在运行（$LOCK），退出"; exit 1
fi
trap 'rmdir "$LOCK"' EXIT

mkdir -p "$ROOT/logs"
export LOG="$ROOT/logs/pipeline_${DT}.log"
echo "===== $(date '+%F %T') dt=$DT report_date=$REPORT_DATE =====" | tee -a "$LOG"

python "$ROOT/data_gen/generate_data.py" --dt "$DT" $EXTRA | tee -a "$LOG" || exit 1
for step in ods_load.py dwd_clean.py dim_dws.py dq_check.py ads_export.py; do
  bash "$ROOT/scripts/run_step.sh" "$step" "$DT" || { echo "[FAIL] $step" | tee -a "$LOG"; exit 1; }
done
bash "$ROOT/scripts/run_step.sh" dq_gate.py "$DT"
rc=$?
echo "===== 结束，退出码 $rc =====" | tee -a "$LOG"
exit $rc
