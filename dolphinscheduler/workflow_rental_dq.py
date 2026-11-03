"""DolphinScheduler 工作流定义（PyDolphinScheduler，代码即工作流）。

每天凌晨 2:00 处理前一天（$[yyyy-MM-dd-1]）的数据：
    datax_sync ─► dwd_clean ─► dim_dws ─► dq_check ─► ads_export ─► dq_gate
dq_gate 退出码为 2（有 error 级数据问题）时任务失败，触发失败告警，下游不会被放行。
dq_check 失败重试 1 次；其余任务重试 2 次、间隔 5 分钟。

提交：pip install apache-dolphinscheduler && python dolphinscheduler/workflow_rental_dq.py
（需先在 DolphinScheduler 里建好租户、项目，并开启 Python Gateway）
"""
from pydolphinscheduler.core.workflow import Workflow
from pydolphinscheduler.tasks.shell import Shell

PROJECT_HOME = "/opt/rental-dq-bigdata"
DT = "$[yyyy-MM-dd-1]"


def step(name: str, script: str, retry: int = 2) -> Shell:
    return Shell(
        name=name,
        command=f"cd {PROJECT_HOME} && SPARK_ON_YARN=1 bash scripts/run_step.sh {script} {DT}",
        fail_retry_times=retry,
        fail_retry_interval=5,
    )


with Workflow(
    name="rental_dq_daily",
    description="租赁数据质量日任务：DataX 同步 → Spark 清洗建模 → 质量检查 → 导出 → 质量门禁",
    schedule="0 0 2 * * ? *",
    start_time="2026-11-01",
    timezone="Asia/Shanghai",
    project="rental_dq",
    tenant="bigdata",
) as workflow:
    datax_sync = Shell(
        name="datax_sync",
        command=f"cd {PROJECT_HOME} && bash scripts/run_datax.sh {DT}",
        fail_retry_times=2,
        fail_retry_interval=5,
    )
    dwd_clean = step("dwd_clean", "dwd_clean.py")
    dim_dws = step("dim_dws", "dim_dws.py")
    dq_check = step("dq_check", "dq_check.py", retry=1)
    ads_export = step("ads_export", "ads_export.py")
    dq_gate = step("dq_gate", "dq_gate.py", retry=0)

    datax_sync >> dwd_clean >> dim_dws >> dq_check >> ads_export >> dq_gate

    workflow.submit()
