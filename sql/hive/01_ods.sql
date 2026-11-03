-- ODS 层：原样落地，全部字段按 STRING 存，带来源文件名和行号，保证每一行都能追溯回源表
-- 按天分区（dt = 业务日期），TEXTFILE + Tab 分隔，和 DataX hdfswriter 的输出格式一致

CREATE DATABASE IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.ods_property (
    property_address STRING COMMENT '房产地址（原始写法）',
    in_management    STRING COMMENT '是否在管 Y/N',
    bedrooms         STRING COMMENT '卧室数',
    src_file         STRING COMMENT '来源文件/来源表',
    src_row_no       BIGINT COMMENT '来源行号',
    load_ts          STRING COMMENT '入仓时间'
) COMMENT '房产清单-原始层'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS ods.ods_tenancy (
    tenancy_id       STRING COMMENT '租约编号',
    property_address STRING COMMENT '房产地址（原始写法）',
    tenant_name      STRING COMMENT '租客姓名（敏感，只保留在 ODS）',
    start_date       STRING COMMENT '起租日（原始写法）',
    end_date         STRING COMMENT '到期日（原始写法）',
    weekly_rent      STRING COMMENT '周租金（原始写法）',
    src_file         STRING,
    src_row_no       BIGINT,
    load_ts          STRING
) COMMENT '租约-原始层（多个版本的工作簿 + 业务录入表）'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS ods.ods_water_bill (
    bill_id          STRING COMMENT '水费账单编号',
    property_address STRING,
    period_end       STRING COMMENT '账单周期结束日',
    amount           STRING COMMENT '金额',
    invoiced_date    STRING COMMENT '向租客开票日期，空表示未开票',
    src_file         STRING,
    src_row_no       BIGINT,
    load_ts          STRING
) COMMENT '水费账单-原始层'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS ods.ods_inspection (
    inspection_id    STRING,
    property_address STRING,
    inspection_type  STRING COMMENT 'routine / entry / exit',
    inspection_date  STRING,
    notice_date      STRING COMMENT '通知租客日期，可能只写了 日.月',
    src_file         STRING,
    src_row_no       BIGINT,
    load_ts          STRING
) COMMENT '房屋检查记录-原始层'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
