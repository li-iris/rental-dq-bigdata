-- 业务库（MySQL）源表结构：物业管理系统录入的数据。DataX 按 update_time 抽取到 Hive ODS。
CREATE DATABASE IF NOT EXISTS rental_ops DEFAULT CHARSET utf8mb4;
USE rental_ops;

CREATE TABLE IF NOT EXISTS property (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  property_address VARCHAR(200), in_management VARCHAR(4), bedrooms VARCHAR(8),
  update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_update_time (update_time)
);
CREATE TABLE IF NOT EXISTS tenancy (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  tenancy_id VARCHAR(32), property_address VARCHAR(200), tenant_name VARCHAR(100),
  start_date VARCHAR(32), end_date VARCHAR(32), weekly_rent VARCHAR(32),
  update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_update_time (update_time)
);
CREATE TABLE IF NOT EXISTS water_bill (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  bill_id VARCHAR(32), property_address VARCHAR(200), period_end VARCHAR(32),
  amount VARCHAR(32), invoiced_date VARCHAR(32),
  update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_update_time (update_time)
);
CREATE TABLE IF NOT EXISTS inspection (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  inspection_id VARCHAR(32), property_address VARCHAR(200), inspection_type VARCHAR(16),
  inspection_date VARCHAR(32), notice_date VARCHAR(32),
  update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_update_time (update_time)
);
