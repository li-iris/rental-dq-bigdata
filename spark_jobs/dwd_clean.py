"""DWD 清洗：地址标准化成 property_key、日期/金额解析、敏感字段脱敏、多来源取生效版本。

原则：不删行。解析不了的字段置空，并把状态写进 address_parse_ok / date_status，交给 DQ 规则报出来。
"""
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from common import get_spark, insert_overwrite, load_conf, parse_args, run_sql_file
from lib.rules import normalize_address, parse_amount, parse_date

ADDR_T = StructType([StructField("key", StringType()), StructField("ok", BooleanType())])
DATE_T = StructType([StructField("d", StringType()), StructField("status", StringType())])


def main():
    args = parse_args("DWD 清洗")
    rules = load_conf("business_rules.json")
    max_ahead = int(rules["date_no_year_max_ahead_days"])

    addr_udf = F.udf(lambda s: normalize_address(s), ADDR_T)
    date_udf = F.udf(lambda s, anchor: parse_date(s, anchor, max_ahead), DATE_T)
    amount_udf = F.udf(parse_amount, DoubleType())

    spark = get_spark("rental_dq_dwd_clean")
    run_sql_file(spark, "sql/hive/02_dwd_dim.sql")
    dt = args.dt
    no_anchor = F.lit(None).cast("string")

    def ods(table):
        return spark.table(table).where(F.col("dt") == dt)

    def with_addr(df):
        return (
            df.withColumn("_a", addr_udf("property_address"))
            .withColumn("property_key", F.col("_a.key"))
            .withColumn("address_parse_ok", F.col("_a.ok").cast("int"))
        )

    # ---------- 房产 ----------
    p = with_addr(ods("ods.ods_property")).select(
        "property_key",
        F.col("property_address").alias("address_raw"),
        "address_parse_ok",
        F.when(F.upper(F.trim("in_management")) == "Y", 1).otherwise(0).alias("in_management"),
        F.col("bedrooms").cast("int").alias("bedrooms"),
        "src_file", "src_row_no",
    )
    print(f"[DWD] dwd_property_df: {insert_overwrite(spark, p, 'dwd.dwd_property_df', dt)} 行")

    # ---------- 租约：多个来源，按优先级取生效版本 ----------
    prio = rules["source_priority"]
    prio_map = F.create_map(*[F.lit(x) for kv in prio.items() for x in kv])
    t = with_addr(ods("ods.ods_tenancy"))
    t = t.withColumn("_sd", date_udf("start_date", no_anchor))
    t = t.withColumn("_ed", date_udf("end_date", F.col("_sd.d")))  # 到期日缺年份时以起租日为锚点
    t = t.withColumn("src_priority", F.coalesce(prio_map[F.col("src_file")], F.lit(0)).cast("int"))
    w = Window.partitionBy("tenancy_id").orderBy(F.col("src_priority").desc(), F.col("src_row_no").desc())
    t = t.select(
        "tenancy_id", "property_key", "address_parse_ok",
        F.when(F.col("tenant_name").isNotNull(), F.sha2(F.col("tenant_name"), 256)).alias("tenant_hash"),
        F.to_date("_sd.d").alias("start_date"),
        F.to_date("_ed.d").alias("end_date"),
        amount_udf("weekly_rent").cast("decimal(10,2)").alias("weekly_rent"),
        F.concat(F.lit("start:"), F.col("_sd.status"), F.lit(";end:"), F.col("_ed.status")).alias("date_status"),
        "src_file", "src_row_no", "src_priority",
        F.when(F.row_number().over(w) == 1, 1).otherwise(0).alias("is_current"),
    )
    print(f"[DWD] dwd_tenancy_df: {insert_overwrite(spark, t, 'dwd.dwd_tenancy_df', dt)} 行")

    # ---------- 水费 ----------
    b = with_addr(ods("ods.ods_water_bill"))
    b = b.withColumn("_pe", date_udf("period_end", no_anchor))
    b = b.withColumn("_inv", date_udf("invoiced_date", F.col("_pe.d")))
    b = b.select(
        "bill_id", "property_key", "address_parse_ok",
        F.to_date("_pe.d").alias("period_end"),
        amount_udf("amount").cast("decimal(12,2)").alias("amount"),
        F.to_date("_inv.d").alias("invoiced_date"),
        F.concat(F.lit("period_end:"), F.col("_pe.status"), F.lit(";invoiced:"), F.col("_inv.status")).alias("date_status"),
        "src_file", "src_row_no",
    )
    print(f"[DWD] dwd_water_bill_df: {insert_overwrite(spark, b, 'dwd.dwd_water_bill_df', dt)} 行")

    # ---------- 房屋检查：通知日期常只写"日.月"，以检查日期为锚点推断年份 ----------
    i = with_addr(ods("ods.ods_inspection"))
    i = i.withColumn("_id", date_udf("inspection_date", no_anchor))
    i = i.withColumn("_nd", date_udf("notice_date", F.col("_id.d")))
    i = i.select(
        "inspection_id", "property_key", "address_parse_ok",
        F.lower(F.trim("inspection_type")).alias("inspection_type"),
        F.to_date("_id.d").alias("inspection_date"),
        F.to_date("_nd.d").alias("notice_date"),
        F.concat(F.lit("inspection:"), F.col("_id.status"), F.lit(";notice:"), F.col("_nd.status")).alias("date_status"),
        "src_file", "src_row_no",
    )
    print(f"[DWD] dwd_inspection_df: {insert_overwrite(spark, i, 'dwd.dwd_inspection_df', dt)} 行")

    spark.stop()


if __name__ == "__main__":
    main()
