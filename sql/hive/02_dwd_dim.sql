-- DWD 层：清洗、类型转换、地址统一成 property_key；不删行，解析不了的字段置空并记状态
-- DIM 层：房产维度，只有在管房产才建维度，其他表只关联不新建
-- 存储：ORC

CREATE DATABASE IF NOT EXISTS dwd;
CREATE DATABASE IF NOT EXISTS dim;

CREATE TABLE IF NOT EXISTS dwd.dwd_property_df (
    property_key      STRING COMMENT '标准化地址键，解析失败为 NULL',
    address_raw       STRING,
    address_parse_ok  INT    COMMENT '1=地址解析成功',
    in_management     INT    COMMENT '1=在管',
    bedrooms          INT,
    src_file          STRING,
    src_row_no        BIGINT
) COMMENT '房产清单-明细层（每日全量快照）'
PARTITIONED BY (dt STRING)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS dwd.dwd_tenancy_df (
    tenancy_id        STRING,
    property_key      STRING,
    address_parse_ok  INT,
    tenant_hash       STRING COMMENT '租客姓名 SHA-256，明细层起不出现姓名',
    start_date        DATE,
    end_date          DATE,
    weekly_rent       DECIMAL(10,2),
    date_status       STRING COMMENT 'start/end 日期解析状态',
    src_file          STRING,
    src_row_no        BIGINT,
    src_priority      INT    COMMENT '来源优先级，业务录入表 99 最高',
    is_current        INT    COMMENT '同一 tenancy_id 取优先级最高的一行 = 1'
) COMMENT '租约-明细层（保留全部来源行，is_current 标记生效版本）'
PARTITIONED BY (dt STRING)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS dwd.dwd_water_bill_df (
    bill_id           STRING,
    property_key      STRING,
    address_parse_ok  INT,
    period_end        DATE,
    amount            DECIMAL(12,2),
    invoiced_date     DATE,
    date_status       STRING,
    src_file          STRING,
    src_row_no        BIGINT
) COMMENT '水费账单-明细层'
PARTITIONED BY (dt STRING)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS dwd.dwd_inspection_df (
    inspection_id     STRING,
    property_key      STRING,
    address_parse_ok  INT,
    inspection_type   STRING,
    inspection_date   DATE,
    notice_date       DATE,
    date_status       STRING,
    src_file          STRING,
    src_row_no        BIGINT
) COMMENT '房屋检查-明细层'
PARTITIONED BY (dt STRING)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS dim.dim_property (
    property_id       STRING COMMENT 'md5(property_key)',
    property_key      STRING,
    bedrooms          INT
) COMMENT '房产维度（每日全量快照）'
PARTITIONED BY (dt STRING)
STORED AS ORC;
