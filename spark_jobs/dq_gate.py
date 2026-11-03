"""质量门禁：当天有 error 级问题时以退出码 2 结束，调度系统据此判失败、不放行下游并告警。

退出码：0 = 通过；1 = 程序异常 / 当天没有检查结果；2 = 存在 error 级数据问题
"""
import sys

from common import get_spark, parse_args


def main():
    args = parse_args("质量门禁")
    spark = get_spark("rental_dq_gate")
    rows = spark.sql(f"""
        SELECT level, sum(fail_cnt) AS n, count(*) AS rules
        FROM ads.ads_dq_result WHERE dt = '{args.dt}' GROUP BY level
    """).collect()
    trend = spark.sql("""
        SELECT dt, sum(CASE WHEN level = 'error' THEN fail_cnt ELSE 0 END) AS errors,
               sum(CASE WHEN level = 'warn'  THEN fail_cnt ELSE 0 END) AS warns
        FROM ads.ads_dq_result GROUP BY dt ORDER BY dt DESC LIMIT 7
    """).collect()
    spark.stop()

    if not rows:
        print(f"[GATE] dt={args.dt} 没有检查结果，先运行 dq_check.py")
        sys.exit(1)
    counts = {r["level"]: int(r["n"] or 0) for r in rows}
    print("[GATE] 最近 7 次问题数趋势（dt / error / warn）：")
    for r in trend:
        print(f"         {r['dt']}  {r['errors']:>4}  {r['warns']:>4}")
    if counts.get("error", 0) > 0:
        print(f"[GATE] dt={args.dt} 存在 {counts['error']} 行 error 级问题，不放行下游")
        sys.exit(2)
    print(f"[GATE] dt={args.dt} 通过（warn {counts.get('warn', 0)} 行已进入问题清单）")


if __name__ == "__main__":
    main()
