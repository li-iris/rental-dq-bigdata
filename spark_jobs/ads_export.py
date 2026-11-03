"""导出给业务方的文件（UTF-8 BOM 的 CSV，Excel 直接打开不乱码）：
- output/problem_list_<dt>.csv：问题清单，error 在前，带"业务方回复/是否已处理"两列，业务方填完回传即可闭环
- output/dq_summary_<dt>.csv：每条规则的失败行数
- output/rent_roll_<month>.csv：报告月的租金应收
"""
import csv

from common import ROOT, get_spark, parse_args

OUT = ROOT / "output"


def write(path, header, rows):
    OUT.mkdir(exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"[ADS] 导出 {path.relative_to(ROOT)}：{len(rows)} 行")


def main():
    args = parse_args("导出报表")
    spark = get_spark("rental_dq_export")
    dt, rd = args.dt, args.report_date

    problems = spark.sql(f"""
        SELECT rule_id, level, rule_desc, property_key, detail, owner_reply, handled
        FROM ads.ads_problem_list WHERE dt = '{dt}'
        ORDER BY CASE level WHEN 'error' THEN 0 ELSE 1 END, rule_id, property_key
    """).collect()
    write(OUT / f"problem_list_{dt}.csv",
          ["规则编号", "级别", "问题说明", "房产", "明细", "业务方回复", "是否已处理"],
          [list(r) for r in problems])

    summary = spark.sql(f"""
        SELECT rule_id, level, rule_desc, fail_cnt FROM ads.ads_dq_result WHERE dt = '{dt}' ORDER BY rule_id
    """).collect()
    write(OUT / f"dq_summary_{dt}.csv", ["规则编号", "级别", "规则说明", "失败行数"], [list(r) for r in summary])

    rent = spark.sql(f"""
        SELECT property_key, month_start, active_tenancy_cnt, weekly_rent_sum, rent_basis, monthly_rent_est
        FROM dws.dws_rent_roll_month
        WHERE dt = '{dt}' AND month_start = trunc(to_date('{rd}'), 'MM')
        ORDER BY property_key
    """).collect()
    month = rd[:7]
    write(OUT / f"rent_roll_{month}.csv",
          ["房产", "月份", "有效租约数", "周租合计", "租金口径", "月租估算"],
          [[r[0], str(r[1]), r[2], r[3], r[4], r[5]] for r in rent])
    spark.stop()


if __name__ == "__main__":
    main()
