-- DWS 层：按 房产 × 月 汇总租金应收
-- ADS 层：数据质量结果、问题清单（给业务方跟进）

CREATE DATABASE IF NOT EXISTS dws;
CREATE DATABASE IF NOT EXISTS ads;

CREATE TABLE IF NOT EXISTS dws.dws_rent_roll_month (
    property_id         STRING,
    property_key        STRING,
    month_start         DATE,
    active_tenancy_cnt  INT,
    weekly_rent_sum     DECIMAL(12,2),
    rent_basis          STRING COMMENT 'no_tenancy / room_rents / unit_rent / room_rent / unclear',
    monthly_rent_est    DECIMAL(12,2) COMMENT '周租 × 52 / 12'
) COMMENT '租金应收-房产月汇总'
PARTITIONED BY (dt STRING)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS ads.ads_dq_result (
    rule_id     STRING,
    level       STRING COMMENT 'error=数据坏了，流程中止；warn=业务需跟进',
    rule_desc   STRING,
    fail_cnt    BIGINT,
    run_ts      STRING
) COMMENT '数据质量检查结果（按天分区 = 历史趋势）'
PARTITIONED BY (dt STRING)
STORED AS ORC;

CREATE TABLE IF NOT EXISTS ads.ads_problem_list (
    rule_id       STRING,
    level         STRING,
    rule_desc     STRING,
    property_key  STRING,
    detail        STRING,
    owner_reply   STRING COMMENT '业务方回复，导出后由业务方填写',
    handled       STRING COMMENT '是否已处理'
) COMMENT '问题清单（失败明细）'
PARTITIONED BY (dt STRING)
STORED AS ORC;
