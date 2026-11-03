"""数据质量检查：逐条执行 conf/dq_rules.json 里的规则 SQL，
汇总写 ads_dq_result（按天分区，可看问题数趋势），失败明细写 ads_problem_list。

本脚本只负责"检查并记录"，正常结束退出码 0；是否放行下游由 dq_gate.py 决定。
"""
from datetime import datetime

from pyspark.sql import functions as F

from common import get_spark, insert_overwrite, load_conf, parse_args, run_sql_file


def render(text: str, params: dict) -> str:
    for k, v in params.items():
        text = text.replace("${" + k + "}", str(v))
    return text


def main():
    args = parse_args("数据质量检查")
    rules = load_conf("business_rules.json")
    dq = load_conf("dq_rules.json")
    params = {k: v for k, v in rules.items() if not k.startswith("_") and not isinstance(v, (dict, list))}
    params.update(dt=args.dt, report_date=args.report_date)

    spark = get_spark("rental_dq_check")
    run_sql_file(spark, "sql/hive/03_dws_ads.sql")
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    summary, problems = [], None
    for r in dq["rules"]:
        desc = render(r["rule_desc"], params)
        fails = spark.sql(render(r["sql"], params)).select(
            F.col("property_key").cast("string").alias("property_key"),
            F.col("detail").cast("string").alias("detail"),
        ).cache()
        cnt = fails.count()
        summary.append((r["rule_id"], r["level"], desc, cnt, run_ts))
        print(f"  [{r['level'].upper():5}] {r['rule_id']:<36} 失败 {cnt:>4} 行  {desc}")
        rows = fails.select(
            F.lit(r["rule_id"]).alias("rule_id"),
            F.lit(r["level"]).alias("level"),
            F.lit(desc).alias("rule_desc"),
            "property_key", "detail",
            F.lit(None).cast("string").alias("owner_reply"),
            F.lit("N").alias("handled"),
        )
        problems = rows if problems is None else problems.unionByName(rows)

    res = spark.createDataFrame(
        summary, "rule_id string, level string, rule_desc string, fail_cnt bigint, run_ts string"
    )
    insert_overwrite(spark, res, "ads.ads_dq_result", args.dt)
    n = insert_overwrite(spark, problems, "ads.ads_problem_list", args.dt)

    err = sum(c for _, lvl, _, c, _ in summary if lvl == "error")
    warn = sum(c for _, lvl, _, c, _ in summary if lvl == "warn")
    print(f"[DQ] dt={args.dt} report_date={args.report_date}: error {err} 行, warn {warn} 行, 问题清单 {n} 行")
    spark.stop()


if __name__ == "__main__":
    main()
