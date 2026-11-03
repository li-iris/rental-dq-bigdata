"""DIM + DWS：
- dim_property：只有在管房产才建维度，property_id = md5(property_key)
- dws_rent_roll_month：房产 × 月 的租金应收；口径说不清（有租约但租金缺失）时标记 unclear，不硬算
"""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common import get_spark, insert_overwrite, load_conf, parse_args, run_sql_file
from lib.rules import rent_basis


def main():
    args = parse_args("DIM + DWS")
    rules = load_conf("business_rules.json")
    threshold = float(rules["single_room_rent_threshold"])
    n_months = int(rules["rent_roll_months"])

    spark = get_spark("rental_dq_dim_dws")
    run_sql_file(spark, "sql/hive/02_dwd_dim.sql")
    run_sql_file(spark, "sql/hive/03_dws_ads.sql")
    dt, rd = args.dt, args.report_date

    # ---------- dim_property ----------
    dim = spark.sql(f"""
        SELECT md5(property_key) AS property_id, property_key, max(bedrooms) AS bedrooms
        FROM dwd.dwd_property_df
        WHERE dt = '{dt}' AND in_management = 1 AND property_key IS NOT NULL
        GROUP BY property_key
    """)
    print(f"[DIM] dim_property: {insert_overwrite(spark, dim, 'dim.dim_property', dt)} 行")

    # ---------- dws_rent_roll_month ----------
    agg = spark.sql(f"""
        WITH m AS (
            SELECT explode(sequence(add_months(trunc(to_date('{rd}'), 'MM'), -{n_months - 1}),
                                    trunc(to_date('{rd}'), 'MM'), interval 1 month)) AS month_start
        ),
        p AS (SELECT property_id, property_key FROM dim.dim_property WHERE dt = '{dt}'),
        t AS (
            SELECT property_key, start_date, end_date, weekly_rent
            FROM dwd.dwd_tenancy_df
            WHERE dt = '{dt}' AND is_current = 1 AND property_key IS NOT NULL
              AND start_date IS NOT NULL AND (end_date IS NULL OR end_date >= start_date)
        ),
        pm AS (SELECT p.property_id, p.property_key, m.month_start, last_day(m.month_start) AS month_end
               FROM p CROSS JOIN m)
        SELECT pm.property_id, pm.property_key, pm.month_start,
               count(t.property_key) AS active_tenancy_cnt,
               sum(t.weekly_rent)    AS weekly_rent_sum,
               max(CASE WHEN t.property_key IS NOT NULL AND t.weekly_rent IS NULL THEN 1 ELSE 0 END) AS has_missing_rent
        FROM pm
        LEFT JOIN t
          ON t.property_key = pm.property_key
         AND t.start_date <= pm.month_end
         AND (t.end_date IS NULL OR t.end_date >= pm.month_start)
        GROUP BY pm.property_id, pm.property_key, pm.month_start
    """)

    # 口径判断和单元测试用的是同一个函数（lib.rules.rent_basis）
    basis_udf = F.udf(
        lambda cnt, rent, missing: rent_basis(cnt, None if (missing or rent is None) else float(rent), threshold),
        StringType(),
    )
    dws = (
        agg.withColumn("rent_basis", basis_udf("active_tenancy_cnt", "weekly_rent_sum", "has_missing_rent"))
        .withColumn("active_tenancy_cnt", F.col("active_tenancy_cnt").cast("int"))
        .withColumn("weekly_rent_sum", F.col("weekly_rent_sum").cast("decimal(12,2)"))
        .withColumn(
            "monthly_rent_est",
            F.when(F.col("rent_basis").isin("unclear", "no_tenancy"), F.lit(None))
            .otherwise(F.round(F.col("weekly_rent_sum") * 52 / 12, 2))
            .cast("decimal(12,2)"),
        )
    )
    print(f"[DWS] dws_rent_roll_month: {insert_overwrite(spark, dws, 'dws.dws_rent_roll_month', dt)} 行")
    spark.stop()


if __name__ == "__main__":
    main()
