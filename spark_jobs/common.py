"""公共方法：参数解析、配置读取、SparkSession、执行 Hive SQL 文件、按分区覆盖写入。"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = ROOT / "spark_jobs"

# 让 Python UDF 在 executor 端也能 import lib.rules（local 模式下 worker 继承这个环境变量）
os.environ["PYTHONPATH"] = os.pathsep.join(p for p in [str(JOBS_DIR), os.environ.get("PYTHONPATH", "")] if p)
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))


def parse_args(desc: str = "") -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=desc)
    ap.add_argument("--dt", required=True, help="业务日期分区 yyyy-MM-dd")
    ap.add_argument("--report-date", default=None, help="判断逾期用的报告日期，默认等于 dt，可回放历史某天")
    args = ap.parse_args()
    args.report_date = args.report_date or args.dt
    return args


def load_conf(name: str) -> dict:
    with open(ROOT / "conf" / name, encoding="utf-8") as f:
        return json.load(f)


def get_spark(app: str):
    """本地模式：Spark 自带的 Hive 支持 + Derby 元数据库，表落在 ./warehouse。
    集群模式：设置环境变量 SPARK_ON_YARN=1，用 spark-submit --master yarn 提交，读集群的 Hive Metastore。"""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app)
    if not os.environ.get("SPARK_ON_YARN"):
        builder = (
            builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
            .config("spark.sql.warehouse.dir", str(ROOT / "warehouse"))
            .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                    f"jdbc:derby:;databaseName={ROOT / 'metastore_db'};create=true")
            .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={ROOT}")
            .config("spark.sql.shuffle.partitions", "4")
        )
    builder = builder.config("hive.exec.dynamic.partition.mode", "nonstrict")
    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_sql_file(spark, rel_path: str) -> None:
    """执行 sql/hive 下的 DDL 文件（去掉 -- 注释行，按分号切分）。"""
    text = (ROOT / rel_path).read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    for stmt in "\n".join(lines).split(";"):
        if stmt.strip():
            spark.sql(stmt)


def insert_overwrite(spark, df, table: str, dt: str) -> int:
    """按目标表字段顺序选列，INSERT OVERWRITE 到 dt 分区（重跑幂等）。返回写入行数。"""
    cols = [f.name for f in spark.table(table).schema.fields if f.name != "dt"]
    view = "_v_" + table.replace(".", "_")
    df.select(*cols).createOrReplaceTempView(view)
    spark.sql(f"INSERT OVERWRITE TABLE {table} PARTITION (dt='{dt}') SELECT {', '.join(cols)} FROM {view}")
    return spark.table(table).where(f"dt = '{dt}'").count()
