"""ODS 落地（本地模式）：检查表头 → CSV 全部按文本读入 → 加来源文件名/行号/入仓时间 → 写 ODS 分区。

生产环境这一步由 DataX 从业务库 MySQL 同步到 HDFS（见 datax/），本脚本用于本地跑通全流程。
"""
import csv
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import ROOT, get_spark, insert_overwrite, load_conf, parse_args, run_sql_file


def check_header(path, expected):
    """表头对不上（有人插列、改列名）就中止，避免错位的数据进仓。"""
    with open(path, encoding="utf-8-sig") as f:
        header = [h.strip() for h in next(csv.reader(f))]
    if header != expected:
        sys.exit(f"[表头检查失败] {path.name}\n  期望: {expected}\n  实际: {header}")


def main():
    args = parse_args("ODS 落地")
    sources = {k: v for k, v in load_conf("sources.json").items() if not k.startswith("_")}
    landing = ROOT / "data" / "landing" / f"dt={args.dt}"

    # 先把所有文件的表头检查完，有问题一行都不写
    plan = {}
    for name, s in sources.items():
        files = sorted(landing.glob(s["file_glob"]))
        if not files:
            sys.exit(f"[缺少源文件] {landing}/{s['file_glob']}")
        for f in files:
            check_header(f, s["columns"])
        plan[name] = files

    spark = get_spark("rental_dq_ods_load")
    run_sql_file(spark, "sql/hive/01_ods.sql")

    for name, files in plan.items():
        s = sources[name]
        df = (
            spark.read.option("header", "true").option("inferSchema", "false")
            .option("encoding", "UTF-8").csv([str(f) for f in files])
        )
        df = (
            df.withColumn("src_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
            .withColumn("_mid", F.monotonically_increasing_id())
        )
        w = Window.partitionBy("src_file").orderBy("_mid")
        df = (
            df.withColumn("src_row_no", (F.row_number().over(w) + 1).cast("bigint"))  # +1：第 1 行是表头
            .withColumn("load_ts", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        )
        n = insert_overwrite(spark, df, s["ods_table"], args.dt)
        print(f"[ODS] {s['ods_table']} dt={args.dt}: {n} 行（{len(files)} 个文件）")

    spark.stop()


if __name__ == "__main__":
    main()
