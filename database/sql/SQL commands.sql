-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
-- -----------------------------------------------------
-- Schema virtual_pm
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema virtual_pm
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `virtual_pm` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ;
USE `virtual_pm` ;

-- -----------------------------------------------------
-- Table `virtual_pm`.`contact`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`contact` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  `vendor_status` ENUM('PENDING', 'TO VERIFY', 'APPROVED', 'ISSUE') NOT NULL DEFAULT 'PENDING',
  `payment_details` VARCHAR(255) NOT NULL DEFAULT 'PENDING',
  `vendor_type` VARCHAR(45) NULL DEFAULT NULL,
  `email` VARCHAR(100) NULL DEFAULT NULL,
  `phone` VARCHAR(45) NULL DEFAULT NULL,
  `address_line_1` VARCHAR(255) NULL DEFAULT NULL,
  `address_line_2` VARCHAR(255) NULL DEFAULT NULL,
  `city` VARCHAR(100) NULL DEFAULT NULL,
  `zip` VARCHAR(20) NULL DEFAULT NULL,
  `region` VARCHAR(45) NULL DEFAULT NULL,
  `country` VARCHAR(100) NULL DEFAULT NULL,
  `tax_type` VARCHAR(45) NULL DEFAULT 'SSN',
  `tax_number` VARCHAR(45) NULL DEFAULT NULL,
  `tax_form_link` VARCHAR(255) NULL DEFAULT NULL,
  `pulse_id` BIGINT NULL DEFAULT NULL,
  `xero_id` VARCHAR(255) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  UNIQUE INDEX `pulse_id` (`pulse_id` ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 4624
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`users`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`users` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `username` VARCHAR(100) NOT NULL,
  `contact_id` BIGINT UNSIGNED NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  INDEX `fk_user_contact` (`contact_id` ASC) VISIBLE,
  CONSTRAINT `fk_user_contact`
    FOREIGN KEY (`contact_id`)
    REFERENCES `virtual_pm`.`contact` (`id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 2
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`budget_map`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`budget_map` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `map_name` VARCHAR(100) NOT NULL,
  `user_id` BIGINT UNSIGNED NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  INDEX `fk_budget_map_users` (`user_id` ASC) VISIBLE,
  CONSTRAINT `fk_budget_map_users`
    FOREIGN KEY (`user_id`)
    REFERENCES `virtual_pm`.`users` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 12
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`tax_ledger`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`tax_ledger` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(100) NOT NULL,
  `user_id` BIGINT UNSIGNED NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  INDEX `fk_tax_ledger_users` (`user_id` ASC) VISIBLE,
  CONSTRAINT `fk_tax_ledger_users`
    FOREIGN KEY (`user_id`)
    REFERENCES `virtual_pm`.`users` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 17
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`tax_account`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`tax_account` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `tax_code` VARCHAR(45) NOT NULL,
  `description` VARCHAR(255) NULL DEFAULT NULL,
  `tax_ledger_id` BIGINT UNSIGNED NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `fk_tax_account_tax_ledger` (`tax_ledger_id` ASC) VISIBLE,
  CONSTRAINT `fk_tax_account_tax_ledger`
    FOREIGN KEY (`tax_ledger_id`)
    REFERENCES `virtual_pm`.`tax_ledger` (`id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 121
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`account_code`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`account_code` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `code` VARCHAR(45) NOT NULL,
  `budget_map_id` BIGINT UNSIGNED NULL DEFAULT NULL,
  `tax_id` BIGINT UNSIGNED NULL DEFAULT NULL,
  `account_description` VARCHAR(45) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `fk_tax_account_idx` (`tax_id` ASC) VISIBLE,
  INDEX `fk_account_code_budget_map` (`budget_map_id` ASC) VISIBLE,
  CONSTRAINT `fk_account_code_budget_map`
    FOREIGN KEY (`budget_map_id`)
    REFERENCES `virtual_pm`.`budget_map` (`id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,
  CONSTRAINT `fk_aicp_code_tax_account`
    FOREIGN KEY (`tax_id`)
    REFERENCES `virtual_pm`.`tax_account` (`id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 16608
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`sys_table`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`sys_table` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(100) NOT NULL,
  `type` ENUM('SYSTEM', 'PARENT/CHILD', 'PARENT', 'CHILD', 'SINGLE') NOT NULL DEFAULT 'SINGLE',
  `integration_name` VARCHAR(45) NULL DEFAULT NULL,
  `integration_type` ENUM('PARENT', 'CHILD', 'SINGLE') NOT NULL DEFAULT 'SINGLE',
  `integration_connection` ENUM('NONE', '1to1', '1toMany', 'Manyto1', 'ManytoMany') NOT NULL DEFAULT 'NONE',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `table_id_uindex` (`id` ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 53
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`audit_log`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`audit_log` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `table_id` BIGINT UNSIGNED NULL DEFAULT NULL,
  `operation` VARCHAR(10) NOT NULL,
  `record_id` BIGINT UNSIGNED NULL DEFAULT NULL,
  `message` VARCHAR(255) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  INDEX `audit_log_sys_table_id_fk` (`table_id` ASC) VISIBLE,
  CONSTRAINT `audit_log_sys_table_id_fk`
    FOREIGN KEY (`table_id`)
    REFERENCES `virtual_pm`.`sys_table` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 389530
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`spend_money`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`spend_money` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_number` INT UNSIGNED NULL DEFAULT NULL,
  `po_number` INT UNSIGNED NULL DEFAULT NULL,
  `detail_number` INT UNSIGNED NULL DEFAULT NULL,
  `line_number` INT UNSIGNED NULL DEFAULT NULL,
  `amount` DECIMAL(10,0) NULL DEFAULT NULL,
  `state` VARCHAR(45) NOT NULL DEFAULT 'Draft',
  `tax_code` INT NULL DEFAULT NULL,
  `xero_link` VARCHAR(255) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `xero_spend_money_id` VARCHAR(100) NULL DEFAULT NULL,
  `xero_spend_money_reference_number` VARCHAR(50) GENERATED ALWAYS AS (concat(lpad(`project_number`,4,_utf8mb4'0'),_utf8mb4'_',lpad(`po_number`,2,_utf8mb4'0'),_utf8mb4'_',lpad(`detail_number`,2,_utf8mb4'0'),_utf8mb4'_',lpad(`line_number`,2,_utf8mb4'0'))) STORED,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 1936
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`bank_transaction`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`bank_transaction` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `mercury_transaction_id` VARCHAR(100) NOT NULL,
  `state` VARCHAR(45) NOT NULL DEFAULT 'Pending',
  `xero_bill_id` BIGINT UNSIGNED NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `xero_spend_money_id` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `mercury_transaction_id` (`mercury_transaction_id` ASC) VISIBLE,
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `fk_xero_bills_idx` (`xero_bill_id` ASC) VISIBLE,
  INDEX `fk_xero_spend_money_idx` (`xero_spend_money_id` ASC) VISIBLE,
  CONSTRAINT `bank_transaction_xero_spend_money_id_fk`
    FOREIGN KEY (`xero_spend_money_id`)
    REFERENCES `virtual_pm`.`spend_money` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`detail_item`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`detail_item` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_number` INT UNSIGNED NULL DEFAULT NULL,
  `po_number` INT UNSIGNED NULL DEFAULT NULL,
  `detail_number` INT UNSIGNED NOT NULL,
  `line_number` INT UNSIGNED NOT NULL,
  `account_code` VARCHAR(45) NOT NULL,
  `vendor` VARCHAR(255) NULL DEFAULT NULL,
  `payment_type` VARCHAR(45) NULL DEFAULT NULL,
  `state` ENUM('PENDING', 'OVERDUE', 'REVIEWED', 'ISSUE', 'RTP', 'RECONCILED', 'PAID', 'APPROVED', 'SUBMITTED', 'PO MISMATCH') NOT NULL DEFAULT 'PENDING',
  `description` VARCHAR(255) NULL DEFAULT NULL,
  `transaction_date` DATETIME NULL DEFAULT NULL,
  `due_date` DATETIME NULL DEFAULT NULL,
  `rate` DECIMAL(15,2) NOT NULL,
  `quantity` DECIMAL(15,2) NOT NULL DEFAULT '1.00',
  `ot` DECIMAL(15,2) NULL DEFAULT '0.00',
  `fringes` DECIMAL(15,2) NULL DEFAULT '0.00',
  `sub_total` DECIMAL(15,2) GENERATED ALWAYS AS (round((((`rate` * `quantity`) + ifnull(`ot`,0)) + ifnull(`fringes`,0)),2)) STORED,
  `receipt_id` INT UNSIGNED NULL DEFAULT NULL,
  `invoice_id` INT UNSIGNED NULL DEFAULT NULL,
  `pulse_id` BIGINT NULL DEFAULT NULL,
  `parent_pulse_id` BIGINT NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  UNIQUE INDEX `detail_item_pulse_id_uindex` (`pulse_id` ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 31346
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`invoice`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`invoice` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_number` INT UNSIGNED NOT NULL,
  `po_number` INT UNSIGNED NOT NULL,
  `invoice_number` INT UNSIGNED NOT NULL,
  `term` INT NULL DEFAULT NULL,
  `total` DECIMAL(15,2) NULL DEFAULT '0.00',
  `transaction_date` DATETIME NULL DEFAULT NULL,
  `file_link` VARCHAR(255) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `po_id` (`po_number` ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 101
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`po_log`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`po_log` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_number` INT UNSIGNED NULL DEFAULT NULL,
  `filename` VARCHAR(255) NULL DEFAULT NULL,
  `db_path` VARCHAR(255) NOT NULL,
  `status` ENUM('PENDING', 'STARTED', 'COMPLETED', 'FAILED') NOT NULL DEFAULT 'PENDING',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 4
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`project`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`project` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_id` INT NULL DEFAULT NULL,
  `project_number` INT NULL DEFAULT NULL,
  `name` VARCHAR(100) NOT NULL,
  `status` ENUM('Active', 'Closed') NOT NULL DEFAULT 'Active',
  `tax_ledger` VARCHAR(45) NULL DEFAULT NULL,
  `budget_map_id` VARCHAR(45) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 2329
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`purchase_order`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`purchase_order` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_number` INT UNSIGNED NULL DEFAULT NULL,
  `po_number` INT UNSIGNED NOT NULL,
  `description` VARCHAR(255) NULL DEFAULT NULL,
  `po_type` VARCHAR(45) NULL DEFAULT NULL,
  `producer` VARCHAR(100) NULL DEFAULT NULL,
  `pulse_id` BIGINT NULL DEFAULT NULL,
  `folder_link` VARCHAR(255) NULL DEFAULT NULL,
  `contact_id` BIGINT UNSIGNED NULL DEFAULT NULL,
  `project_id` BIGINT UNSIGNED NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `project_id_2` (`project_id` ASC) VISIBLE,
  INDEX `fk_purchase_order_contact` (`contact_id` ASC) VISIBLE,
  CONSTRAINT `fk_purchase_order_contact`
    FOREIGN KEY (`contact_id`)
    REFERENCES `virtual_pm`.`contact` (`id`)
    ON DELETE SET NULL,
  CONSTRAINT `fk_purchase_order_project`
    FOREIGN KEY (`project_id`)
    REFERENCES `virtual_pm`.`project` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 4232
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`receipt`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`receipt` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_number` INT UNSIGNED NOT NULL,
  `po_number` INT UNSIGNED NULL DEFAULT NULL,
  `detail_number` INT UNSIGNED NULL DEFAULT NULL,
  `line_number` INT UNSIGNED NULL DEFAULT NULL,
  `receipt_description` VARCHAR(255) NULL DEFAULT NULL,
  `total` DECIMAL(15,2) NULL DEFAULT '0.00',
  `purchase_date` DATETIME NULL DEFAULT NULL,
  `dropbox_path` VARCHAR(255) NULL DEFAULT NULL,
  `file_link` VARCHAR(255) NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `spend_money_id` BIGINT UNSIGNED NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `fk_spend_money_idx` (`spend_money_id` ASC) VISIBLE,
  CONSTRAINT `receipt_spend_money_id_fk`
    FOREIGN KEY (`spend_money_id`)
    REFERENCES `virtual_pm`.`spend_money` (`id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 393
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`tax_form`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`tax_form` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `contact_id` BIGINT UNSIGNED NOT NULL,
  `type` ENUM('W9', 'W8-BEN', 'W8-BEN-E') NOT NULL,
  `filename` VARCHAR(100) NULL DEFAULT NULL,
  `db_path` VARCHAR(255) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `tax_form_contact_id_uindex` (`contact_id` ASC) VISIBLE,
  UNIQUE INDEX `tax_form_id_uindex` (`id` ASC) VISIBLE,
  CONSTRAINT `tax_form_contact_id_fk`
    FOREIGN KEY (`contact_id`)
    REFERENCES `virtual_pm`.`contact` (`id`)
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`xero_bill`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`xero_bill` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `state` VARCHAR(45) NOT NULL DEFAULT 'Draft',
  `project_number` INT UNSIGNED NULL DEFAULT NULL,
  `po_number` INT UNSIGNED NULL DEFAULT NULL,
  `detail_number` INT UNSIGNED NULL DEFAULT NULL,
  `transaction_date` DATE NULL DEFAULT NULL,
  `due_date` DATE NULL DEFAULT NULL,
  `xero_reference_number` VARCHAR(50) GENERATED ALWAYS AS (concat(lpad(`project_number`,4,_utf8mb4'0'),_utf8mb4'_',lpad(`po_number`,2,_utf8mb4'0'),_utf8mb4'_',lpad(`detail_number`,2,_utf8mb4'0'))) STORED,
  `xero_id` VARCHAR(255) NULL DEFAULT NULL,
  `xero_link` VARCHAR(255) NULL DEFAULT NULL,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  UNIQUE INDEX `number` (`project_number` DESC, `po_number` DESC, `detail_number` ASC) VISIBLE,
  UNIQUE INDEX `xero_id` (`xero_reference_number`(45) ASC) VISIBLE)
ENGINE = InnoDB
AUTO_INCREMENT = 2796
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;


-- -----------------------------------------------------
-- Table `virtual_pm`.`xero_bill_line_item`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`xero_bill_line_item` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_number` INT UNSIGNED NULL DEFAULT NULL,
  `po_number` INT UNSIGNED NULL DEFAULT NULL,
  `detail_number` INT UNSIGNED NULL DEFAULT NULL,
  `line_number` INT UNSIGNED NULL DEFAULT NULL,
  `description` VARCHAR(255) NULL DEFAULT NULL,
  `quantity` DECIMAL(10,0) NULL DEFAULT NULL,
  `unit_amount` DECIMAL(10,0) NULL DEFAULT NULL,
  `line_amount` DECIMAL(10,0) NULL DEFAULT NULL,
  `account_code` INT NULL DEFAULT NULL,
  `parent_id` BIGINT UNSIGNED NOT NULL,
  `xero_bill_line_id` VARCHAR(255) NULL DEFAULT NULL,
  `parent_xero_id` VARCHAR(255) NULL DEFAULT NULL,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `transaction_date` DATE NULL DEFAULT NULL,
  `due_date` DATE NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `bill_line_item_xero_bill_id_fk` (`parent_id` ASC) VISIBLE,
  CONSTRAINT `bill_line_item_xero_bill_id_fk`
    FOREIGN KEY (`parent_id`)
    REFERENCES `virtual_pm`.`xero_bill` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 4011
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

USE `virtual_pm` ;

-- -----------------------------------------------------
-- Placeholder table for view `virtual_pm`.`vw_average_po_value_per_project`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`vw_average_po_value_per_project` (`project_id` INT, `project_name` INT, `avg_po_value` INT);

-- -----------------------------------------------------
-- Placeholder table for view `virtual_pm`.`vw_detail_items_extended`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`vw_detail_items_extended` (`detail_item_id` INT, `detail_item_state` INT, `detail_description` INT, `detail_subtotal` INT, `po_id` INT, `po_number` INT, `contact_id` INT, `contact_name` INT, `project_id` INT, `project_number` INT, `project_name` INT, `file_link` INT);

-- -----------------------------------------------------
-- Placeholder table for view `virtual_pm`.`vw_detail_items_summary`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`vw_detail_items_summary` (`po_id` INT, `project_name` INT, `po_number` INT, `total_items` INT, `total_sub_total` INT, `avg_item_cost` INT);

-- -----------------------------------------------------
-- Placeholder table for view `virtual_pm`.`vw_invoice_summary`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`vw_invoice_summary` (`invoice_id` INT, `invoice_number` INT, `invoice_total` INT, `transaction_date` INT, `project_name` INT, `po_number` INT, `created_at` INT, `updated_at` INT);

-- -----------------------------------------------------
-- Placeholder table for view `virtual_pm`.`vw_project_stats`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`vw_project_stats` (`project_id` INT, `project_number` INT, `project_name` INT, `total_pos` INT, `total_detail_items` INT, `total_sum_of_detail_items` INT, `cnt_pending` INT, `cnt_overdue` INT, `cnt_reviewed` INT, `cnt_issue` INT, `cnt_rtp` INT, `cnt_reconciled` INT, `cnt_paid` INT, `cnt_approved` INT, `cnt_submitted` INT, `cnt_po_mismatch` INT);

-- -----------------------------------------------------
-- Placeholder table for view `virtual_pm`.`vw_project_totals`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`vw_project_totals` (`project_id` INT, `project_name` INT, `status` INT, `total_spent` INT, `number_of_purchase_orders` INT);

-- -----------------------------------------------------
-- Placeholder table for view `virtual_pm`.`vw_purchase_order_details`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`vw_purchase_order_details` (`po_id` INT, `project_name` INT, `po_number` INT, `amount_total` INT, `contact_name` INT, `state` INT, `created_at` INT, `updated_at` INT);

-- -----------------------------------------------------
-- Placeholder table for view `virtual_pm`.`vw_xero_bills_line_items`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`vw_xero_bills_line_items` (`xero_bill_id` INT, `xero_bill_state` INT, `xero_reference_number` INT, `bill_line_item_id` INT, `bill_line_desc` INT, `bill_line_amount` INT, `bill_line_account_code` INT, `detail_item_id` INT, `detail_item_state` INT, `detail_item_subtotal` INT, `purchase_order_id` INT, `purchase_order_number` INT, `project_name` INT);

-- -----------------------------------------------------
-- Placeholder table for view `virtual_pm`.`vw_xero_bills_vs_pos`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `virtual_pm`.`vw_xero_bills_vs_pos` (`xero_bill_id` INT, `xero_bill_state` INT, `xero_reference_number` INT, `xero_bill_created_at` INT, `xero_bill_updated_at` INT, `purchase_order_id` INT, `purchase_order_number` INT, `purchase_order_state` INT, `project_id` INT, `project_number` INT, `project_name` INT);

-- -----------------------------------------------------
-- procedure sp_create_investor_views
-- -----------------------------------------------------

DELIMITER $$
USE `virtual_pm`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_create_investor_views`()
BEGIN
    -- 1. Vendor Spend Summary: now join contact <-> purchase_order
    CREATE OR REPLACE VIEW vw_vendor_spend_summary AS
    SELECT
        c.id AS contact_id,
        c.name AS vendor_name,
        COUNT(po.id) AS total_po_count,
        SUM(po.amount_total) AS total_spent,
        AVG(po.amount_total) AS avg_po_amount
    FROM contact c
    JOIN purchase_order po ON po.contact_id = c.id
    GROUP BY c.id, c.name;

    -- 2. Monthly Spending Trends
    CREATE OR REPLACE VIEW vw_monthly_spend_trends AS
    SELECT
        YEAR(po.created_at) AS year,
        MONTH(po.created_at) AS month,
        SUM(po.amount_total) AS total_spent_this_month,
        COUNT(po.id) AS total_po_count
    FROM purchase_order po
    GROUP BY YEAR(po.created_at), MONTH(po.created_at)
    ORDER BY YEAR(po.created_at), MONTH(po.created_at);

    -- 3. Invoice Turnaround Time
    CREATE OR REPLACE VIEW vw_invoice_turnaround AS
    SELECT
        i.id AS invoice_id,
        i.invoice_number,
        p.name AS project_name,
        po.po_number,
        po.created_at AS po_created_at,
        i.transaction_date AS invoice_date,
        DATEDIFF(i.transaction_date, po.created_at) AS days_to_invoice
    FROM invoice i
    JOIN purchase_order po ON i.po_id = po.id
    JOIN project p ON p.id = po.project_id;

    -- 4. Project Vendor Distribution
    --   Using purchase_order.contact_id instead of contact_po
    CREATE OR REPLACE VIEW vw_project_vendor_distribution AS
    SELECT
        p.id AS project_id,
        p.name AS project_name,
        COUNT(DISTINCT c.id) AS distinct_vendors,
        SUM(po.amount_total) AS total_spent
    FROM project p
    JOIN purchase_order po ON p.id = po.project_id
    LEFT JOIN contact c ON c.id = po.contact_id
    GROUP BY p.id, p.name;

    -- 5. Vendor Popularity Across Projects
    CREATE OR REPLACE VIEW vw_vendor_popularity AS
    SELECT
        c.id AS contact_id,
        c.name AS vendor_name,
        COUNT(DISTINCT po.project_id) AS distinct_projects,
        SUM(po.amount_total) AS total_earnings
    FROM contact c
    JOIN purchase_order po ON po.contact_id = c.id
    GROUP BY c.id, c.name;

    -- 6. Average Purchase Order Value Per Project
    CREATE OR REPLACE VIEW vw_average_po_value_per_project AS
    SELECT
        p.id AS project_id,
        p.name AS project_name,
        AVG(po.amount_total) AS avg_po_value
    FROM project p
    JOIN purchase_order po ON p.id = po.project_id
    GROUP BY p.id, p.name;
END$$

DELIMITER ;

-- -----------------------------------------------------
-- View `virtual_pm`.`vw_average_po_value_per_project`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `virtual_pm`.`vw_average_po_value_per_project`;
USE `virtual_pm`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `virtual_pm`.`vw_average_po_value_per_project` AS select `virtual_pm`.`p`.`id` AS `project_id`,`virtual_pm`.`p`.`name` AS `project_name`,avg(`virtual_pm`.`po`.`amount_total`) AS `avg_po_value` from (`virtual_pm`.`projects` `p` join `virtual_pm`.`purchase_orders` `po` on((`virtual_pm`.`p`.`id` = `virtual_pm`.`po`.`project_id`))) group by `virtual_pm`.`p`.`id`,`virtual_pm`.`p`.`name`;

-- -----------------------------------------------------
-- View `virtual_pm`.`vw_detail_items_extended`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `virtual_pm`.`vw_detail_items_extended`;
USE `virtual_pm`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `virtual_pm`.`vw_detail_items_extended` AS select `di`.`id` AS `detail_item_id`,`di`.`state` AS `detail_item_state`,`di`.`description` AS `detail_description`,`di`.`sub_total` AS `detail_subtotal`,`po`.`id` AS `po_id`,`po`.`po_number` AS `po_number`,`c`.`id` AS `contact_id`,`c`.`name` AS `contact_name`,`p`.`id` AS `project_id`,`p`.`project_number` AS `project_number`,`p`.`name` AS `project_name`,coalesce(`r`.`file_link`,`i`.`file_link`) AS `file_link` from (((((`virtual_pm`.`detail_item` `di` join `virtual_pm`.`purchase_order` `po` on((`virtual_pm`.`di`.`po_id` = `virtual_pm`.`po`.`id`))) join `virtual_pm`.`project` `p` on((`po`.`project_id` = `p`.`id`))) left join `virtual_pm`.`contact` `c` on((`po`.`contact_id` = `c`.`id`))) left join `virtual_pm`.`receipt` `r` on((`di`.`receipt_id` = `r`.`id`))) left join `virtual_pm`.`invoice` `i` on((`di`.`invoice_id` = `i`.`id`)));

-- -----------------------------------------------------
-- View `virtual_pm`.`vw_detail_items_summary`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `virtual_pm`.`vw_detail_items_summary`;
USE `virtual_pm`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `virtual_pm`.`vw_detail_items_summary` AS select `po`.`id` AS `po_id`,`p`.`name` AS `project_name`,`po`.`po_number` AS `po_number`,count(`di`.`id`) AS `total_items`,sum(`di`.`sub_total`) AS `total_sub_total`,avg(`di`.`sub_total`) AS `avg_item_cost` from ((`virtual_pm`.`detail_item` `di` join `virtual_pm`.`purchase_order` `po` on((`po`.`id` = `virtual_pm`.`di`.`po_id`))) join `virtual_pm`.`project` `p` on((`p`.`id` = `po`.`project_id`))) group by `virtual_pm`.`po`.`id`,`virtual_pm`.`p`.`name`,`virtual_pm`.`po`.`po_number`;

-- -----------------------------------------------------
-- View `virtual_pm`.`vw_invoice_summary`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `virtual_pm`.`vw_invoice_summary`;
USE `virtual_pm`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `virtual_pm`.`vw_invoice_summary` AS select `i`.`id` AS `invoice_id`,`i`.`invoice_number` AS `invoice_number`,`i`.`total` AS `invoice_total`,`i`.`transaction_date` AS `transaction_date`,`p`.`name` AS `project_name`,`po`.`po_number` AS `po_number`,`i`.`created_at` AS `created_at`,`i`.`updated_at` AS `updated_at` from ((`virtual_pm`.`invoice` `i` join `virtual_pm`.`purchase_order` `po` on((`virtual_pm`.`po`.`id` = `virtual_pm`.`i`.`po_id`))) join `virtual_pm`.`project` `p` on((`p`.`id` = `virtual_pm`.`i`.`project_id`)));

-- -----------------------------------------------------
-- View `virtual_pm`.`vw_project_stats`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `virtual_pm`.`vw_project_stats`;
USE `virtual_pm`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `virtual_pm`.`vw_project_stats` AS select `p`.`id` AS `project_id`,`p`.`project_number` AS `project_number`,`p`.`name` AS `project_name`,count(distinct `po`.`id`) AS `total_pos`,count(distinct `di`.`id`) AS `total_detail_items`,ifnull(sum(`di`.`sub_total`),0) AS `total_sum_of_detail_items`,sum((case when (`di`.`state` = 'PENDING') then 1 else 0 end)) AS `cnt_pending`,sum((case when (`di`.`state` = 'OVERDUE') then 1 else 0 end)) AS `cnt_overdue`,sum((case when (`di`.`state` = 'REVIEWED') then 1 else 0 end)) AS `cnt_reviewed`,sum((case when (`di`.`state` = 'ISSUE') then 1 else 0 end)) AS `cnt_issue`,sum((case when (`di`.`state` = 'RTP') then 1 else 0 end)) AS `cnt_rtp`,sum((case when (`di`.`state` = 'RECONCILED') then 1 else 0 end)) AS `cnt_reconciled`,sum((case when (`di`.`state` = 'PAID') then 1 else 0 end)) AS `cnt_paid`,sum((case when (`di`.`state` = 'APPROVED') then 1 else 0 end)) AS `cnt_approved`,sum((case when (`di`.`state` = 'SUBMITTED') then 1 else 0 end)) AS `cnt_submitted`,sum((case when (`di`.`state` = 'PO MISMATCH') then 1 else 0 end)) AS `cnt_po_mismatch` from ((`virtual_pm`.`project` `p` left join `virtual_pm`.`purchase_order` `po` on((`virtual_pm`.`p`.`id` = `virtual_pm`.`po`.`project_id`))) left join `virtual_pm`.`detail_item` `di` on((`po`.`id` = `virtual_pm`.`di`.`po_id`))) group by `virtual_pm`.`p`.`id`,`virtual_pm`.`p`.`project_number`,`virtual_pm`.`p`.`name`;

-- -----------------------------------------------------
-- View `virtual_pm`.`vw_project_totals`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `virtual_pm`.`vw_project_totals`;
USE `virtual_pm`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `virtual_pm`.`vw_project_totals` AS select `p`.`id` AS `project_id`,`p`.`name` AS `project_name`,`p`.`status` AS `status`,`virtual_pm`.`p`.`total_spent` AS `total_spent`,count(`virtual_pm`.`po`.`id`) AS `number_of_purchase_orders` from (`virtual_pm`.`project` `p` left join `virtual_pm`.`purchase_order` `po` on((`virtual_pm`.`p`.`id` = `virtual_pm`.`po`.`project_id`))) group by `virtual_pm`.`p`.`id`,`virtual_pm`.`p`.`name`,`virtual_pm`.`p`.`status`,`virtual_pm`.`p`.`total_spent`;

-- -----------------------------------------------------
-- View `virtual_pm`.`vw_purchase_order_details`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `virtual_pm`.`vw_purchase_order_details`;
USE `virtual_pm`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `virtual_pm`.`vw_purchase_order_details` AS select `po`.`id` AS `po_id`,`p`.`name` AS `project_name`,`po`.`po_number` AS `po_number`,`po`.`amount_total` AS `amount_total`,`c`.`name` AS `contact_name`,`po`.`state` AS `state`,`po`.`created_at` AS `created_at`,`po`.`updated_at` AS `updated_at` from (((`virtual_pm`.`purchase_order` `po` join `virtual_pm`.`project` `p` on((`p`.`id` = `po`.`project_id`))) left join `virtual_pm`.`contact_po` `cpo` on((`cpo`.`po_id` = `po`.`id`))) left join `virtual_pm`.`contact` `c` on((`c`.`id` = `cpo`.`contact_id`)));

-- -----------------------------------------------------
-- View `virtual_pm`.`vw_xero_bills_line_items`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `virtual_pm`.`vw_xero_bills_line_items`;
USE `virtual_pm`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `virtual_pm`.`vw_xero_bills_line_items` AS select `virtual_pm`.`xb`.`id` AS `xero_bill_id`,`virtual_pm`.`xb`.`state` AS `xero_bill_state`,`virtual_pm`.`xb`.`xero_reference_number` AS `xero_reference_number`,`virtual_pm`.`bli`.`id` AS `bill_line_item_id`,`virtual_pm`.`bli`.`description` AS `bill_line_desc`,`virtual_pm`.`bli`.`line_amount` AS `bill_line_amount`,`virtual_pm`.`bli`.`account_code` AS `bill_line_account_code`,`virtual_pm`.`di`.`id` AS `detail_item_id`,`virtual_pm`.`di`.`state` AS `detail_item_state`,`virtual_pm`.`di`.`sub_total` AS `detail_item_subtotal`,`virtual_pm`.`po`.`id` AS `purchase_order_id`,`virtual_pm`.`po`.`po_number` AS `purchase_order_number`,`virtual_pm`.`p`.`name` AS `project_name` from ((((`virtual_pm`.`xero_bill` `xb` join `virtual_pm`.`bill_line_item` `bli` on((`virtual_pm`.`xb`.`id` = `virtual_pm`.`bli`.`xero_bill_id`))) left join `virtual_pm`.`detail_item` `di` on((`virtual_pm`.`bli`.`detail_item_id` = `virtual_pm`.`di`.`id`))) left join `virtual_pm`.`purchase_order` `po` on((`virtual_pm`.`di`.`po_id` = `virtual_pm`.`po`.`id`))) left join `virtual_pm`.`project` `p` on((`virtual_pm`.`po`.`project_id` = `virtual_pm`.`p`.`id`)));

-- -----------------------------------------------------
-- View `virtual_pm`.`vw_xero_bills_vs_pos`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `virtual_pm`.`vw_xero_bills_vs_pos`;
USE `virtual_pm`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `virtual_pm`.`vw_xero_bills_vs_pos` AS select `xb`.`id` AS `xero_bill_id`,`xb`.`state` AS `xero_bill_state`,`xb`.`xero_reference_number` AS `xero_reference_number`,`xb`.`created_at` AS `xero_bill_created_at`,`xb`.`updated_at` AS `xero_bill_updated_at`,`po`.`id` AS `purchase_order_id`,`po`.`po_number` AS `purchase_order_number`,`virtual_pm`.`po`.`state` AS `purchase_order_state`,`virtual_pm`.`p`.`id` AS `project_id`,`virtual_pm`.`p`.`project_number` AS `project_number`,`virtual_pm`.`p`.`name` AS `project_name` from ((`virtual_pm`.`xero_bill` `xb` left join `virtual_pm`.`purchase_order` `po` on((`virtual_pm`.`xb`.`po_id` = `virtual_pm`.`po`.`id`))) left join `virtual_pm`.`project` `p` on((`virtual_pm`.`po`.`project_id` = `virtual_pm`.`p`.`id`)));
USE `virtual_pm`;

DELIMITER $$
USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`contact_ad`
AFTER DELETE ON `virtual_pm`.`contact`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'contact'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted contact.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`contact_ai`
AFTER INSERT ON `virtual_pm`.`contact`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'contact'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted contact.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`contact_au`
AFTER UPDATE ON `virtual_pm`.`contact`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'contact'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated contact.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`contact_bu`
BEFORE UPDATE ON `virtual_pm`.`contact`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`users_ad`
AFTER DELETE ON `virtual_pm`.`users`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'users'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted users.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`users_ai`
AFTER INSERT ON `virtual_pm`.`users`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'users'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted users.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`users_au`
AFTER UPDATE ON `virtual_pm`.`users`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'users'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated users.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`users_bu`
BEFORE UPDATE ON `virtual_pm`.`users`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`budget_map_ad`
AFTER DELETE ON `virtual_pm`.`budget_map`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'budget_map'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted budget_map.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`budget_map_ai`
AFTER INSERT ON `virtual_pm`.`budget_map`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'budget_map'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted budget_map.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`budget_map_au`
AFTER UPDATE ON `virtual_pm`.`budget_map`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'budget_map'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated budget_map.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`budget_map_bu`
BEFORE UPDATE ON `virtual_pm`.`budget_map`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_ledger_ad`
AFTER DELETE ON `virtual_pm`.`tax_ledger`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_ledger'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted tax_ledger.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_ledger_ai`
AFTER INSERT ON `virtual_pm`.`tax_ledger`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_ledger'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted tax_ledger.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_ledger_au`
AFTER UPDATE ON `virtual_pm`.`tax_ledger`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_ledger'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated tax_ledger.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_ledger_bu`
BEFORE UPDATE ON `virtual_pm`.`tax_ledger`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_account_ad`
AFTER DELETE ON `virtual_pm`.`tax_account`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_account'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted tax_account.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_account_ai`
AFTER INSERT ON `virtual_pm`.`tax_account`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_account'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted tax_account.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_account_au`
AFTER UPDATE ON `virtual_pm`.`tax_account`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_account'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated tax_account.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_account_bu`
BEFORE UPDATE ON `virtual_pm`.`tax_account`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`account_code_ad`
AFTER DELETE ON `virtual_pm`.`account_code`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'account_code'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted account_code.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`account_code_ai`
AFTER INSERT ON `virtual_pm`.`account_code`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'account_code'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted account_code.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`account_code_au`
AFTER UPDATE ON `virtual_pm`.`account_code`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'account_code'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated account_code.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`account_code_bu`
BEFORE UPDATE ON `virtual_pm`.`account_code`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`spend_money_ad`
AFTER DELETE ON `virtual_pm`.`spend_money`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'spend_money'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted spend_money.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`spend_money_ai`
AFTER INSERT ON `virtual_pm`.`spend_money`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'spend_money'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted spend_money.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`spend_money_au`
AFTER UPDATE ON `virtual_pm`.`spend_money`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'spend_money'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated spend_money.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`spend_money_bu`
BEFORE UPDATE ON `virtual_pm`.`spend_money`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`bank_transaction_ad`
AFTER DELETE ON `virtual_pm`.`bank_transaction`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'bank_transaction'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted bank_transaction.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`bank_transaction_ai`
AFTER INSERT ON `virtual_pm`.`bank_transaction`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'bank_transaction'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted bank_transaction.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`bank_transaction_au`
AFTER UPDATE ON `virtual_pm`.`bank_transaction`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'bank_transaction'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated bank_transaction.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`bank_transaction_bu`
BEFORE UPDATE ON `virtual_pm`.`bank_transaction`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`detail_item_ad`
AFTER DELETE ON `virtual_pm`.`detail_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'detail_item'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted detail_item.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`detail_item_ai`
AFTER INSERT ON `virtual_pm`.`detail_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'detail_item'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted detail_item.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`detail_item_au`
AFTER UPDATE ON `virtual_pm`.`detail_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'detail_item'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated detail_item.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`detail_item_bu`
BEFORE UPDATE ON `virtual_pm`.`detail_item`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`invoice_ad`
AFTER DELETE ON `virtual_pm`.`invoice`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'invoice'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted invoice.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`invoice_ai`
AFTER INSERT ON `virtual_pm`.`invoice`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'invoice'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted invoice.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`invoice_au`
AFTER UPDATE ON `virtual_pm`.`invoice`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'invoice'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated invoice.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`invoice_bu`
BEFORE UPDATE ON `virtual_pm`.`invoice`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`po_log_ad`
AFTER DELETE ON `virtual_pm`.`po_log`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'po_log'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted po_log.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`po_log_ai`
AFTER INSERT ON `virtual_pm`.`po_log`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'po_log'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted po_log.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`po_log_au`
AFTER UPDATE ON `virtual_pm`.`po_log`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'po_log'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated po_log.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`po_log_bu`
BEFORE UPDATE ON `virtual_pm`.`po_log`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`project_ad`
AFTER DELETE ON `virtual_pm`.`project`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'project'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted project.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`project_ai`
AFTER INSERT ON `virtual_pm`.`project`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'project'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted project.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`project_au`
AFTER UPDATE ON `virtual_pm`.`project`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'project'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated project.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`project_bu`
BEFORE UPDATE ON `virtual_pm`.`project`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`purchase_order_ad`
AFTER DELETE ON `virtual_pm`.`purchase_order`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'purchase_order'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted purchase_order.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`purchase_order_ai`
AFTER INSERT ON `virtual_pm`.`purchase_order`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'purchase_order'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted purchase_order.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`purchase_order_au`
AFTER UPDATE ON `virtual_pm`.`purchase_order`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'purchase_order'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated purchase_order.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`purchase_order_bu`
BEFORE UPDATE ON `virtual_pm`.`purchase_order`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`receipt_ad`
AFTER DELETE ON `virtual_pm`.`receipt`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'receipt'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted receipt.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`receipt_ai`
AFTER INSERT ON `virtual_pm`.`receipt`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'receipt'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted receipt.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`receipt_au`
AFTER UPDATE ON `virtual_pm`.`receipt`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'receipt'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated receipt.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`receipt_bu`
BEFORE UPDATE ON `virtual_pm`.`receipt`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_form_ad`
AFTER DELETE ON `virtual_pm`.`tax_form`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_form'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted tax_form.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_form_ai`
AFTER INSERT ON `virtual_pm`.`tax_form`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_form'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted tax_form.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_form_au`
AFTER UPDATE ON `virtual_pm`.`tax_form`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_form'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated tax_form.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`tax_form_bu`
BEFORE UPDATE ON `virtual_pm`.`tax_form`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`xero_bill_ad`
AFTER DELETE ON `virtual_pm`.`xero_bill`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'xero_bill'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted xero_bill.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`xero_bill_ai`
AFTER INSERT ON `virtual_pm`.`xero_bill`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'xero_bill'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted xero_bill.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`xero_bill_au`
AFTER UPDATE ON `virtual_pm`.`xero_bill`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'xero_bill'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated xero_bill.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`xero_bill_bu`
BEFORE UPDATE ON `virtual_pm`.`xero_bill`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`bill_line_item_ad`
AFTER DELETE ON `virtual_pm`.`xero_bill_line_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'bill_line_item'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted bill_line_item.id=', OLD.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`bill_line_item_ai`
AFTER INSERT ON `virtual_pm`.`xero_bill_line_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'bill_line_item'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted bill_line_item.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`bill_line_item_au`
AFTER UPDATE ON `virtual_pm`.`xero_bill_line_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'bill_line_item'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated bill_line_item.id=', NEW.id)
    );
END$$

USE `virtual_pm`$$
CREATE
DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`bill_line_item_bu`
BEFORE UPDATE ON `virtual_pm`.`xero_bill_line_item`
FOR EACH ROW
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END$$


DELIMITER ;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
