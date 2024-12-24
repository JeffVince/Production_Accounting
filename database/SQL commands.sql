SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS; 
SET UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS; 
SET FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE; 
SET SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema virtual_pm
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `virtual_pm`
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE `virtual_pm`;

-- -----------------------------------------------------
-- Drop any old tables if needed
-- -----------------------------------------------------
DROP TABLE IF EXISTS `contact_po`;

-- -----------------------------------------------------
-- Table: tax_account
-- -----------------------------------------------------
DROP TABLE IF EXISTS `tax_account`;
CREATE TABLE IF NOT EXISTS `tax_account` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `tax_code` VARCHAR(45) NOT NULL,
  `description` VARCHAR(255) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `tax_account` (`id` ASC);

-- -----------------------------------------------------
-- Table: aicp_code
-- -----------------------------------------------------
DROP TABLE IF EXISTS `aicp_code`;
CREATE TABLE IF NOT EXISTS `aicp_code` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `aicp_code` VARCHAR(45) NOT NULL,
  `tax_id` INT UNSIGNED NOT NULL,
  `aicp_description` VARCHAR(45) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_aicp_code_tax_account`
    FOREIGN KEY (`tax_id`)
    REFERENCES `tax_account` (`id`)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `aicp_code`
  ON `aicp_code` (`aicp_code` ASC);

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `aicp_code` (`id` ASC);

CREATE INDEX `fk_tax_account_idx`
  ON `aicp_code` (`tax_id` ASC);

-- -----------------------------------------------------
-- Table: xero_bill
-- -----------------------------------------------------
DROP TABLE IF EXISTS `xero_bill`;
CREATE TABLE IF NOT EXISTS `xero_bill` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `state` VARCHAR(45) NOT NULL DEFAULT 'Draft',
  `xero_reference_number` VARCHAR(45) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `xero_bill` (`id` ASC);

CREATE UNIQUE INDEX `xero_id`
  ON `xero_bill` (`xero_reference_number` ASC);

-- -----------------------------------------------------
-- Table: spend_money
-- -----------------------------------------------------
DROP TABLE IF EXISTS `spend_money`;
CREATE TABLE IF NOT EXISTS `spend_money` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `xero_spend_money_reference_number` VARCHAR(100) NULL DEFAULT NULL,
  `file_link` VARCHAR(255) NULL DEFAULT NULL,
  `state` VARCHAR(45) NOT NULL DEFAULT 'Draft',
  `spend_moneycol` VARCHAR(45) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `spend_money` (`id` ASC);

CREATE UNIQUE INDEX `xero_spend_money_id`
  ON `spend_money` (`xero_spend_money_reference_number` ASC);

-- -----------------------------------------------------
-- Table: bank_transaction
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bank_transaction`;
CREATE TABLE IF NOT EXISTS `bank_transaction` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `mercury_transaction_id` VARCHAR(100) NOT NULL,
  `state` VARCHAR(45) NOT NULL DEFAULT 'Pending',
  `xero_bill_id` INT UNSIGNED NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `xero_spend_money_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_bank_xero_bills`
    FOREIGN KEY (`xero_bill_id`)
    REFERENCES `xero_bill` (`id`),
  CONSTRAINT `fk_bank_xero_spend_money`
    FOREIGN KEY (`xero_spend_money_id`)
    REFERENCES `spend_money` (`id`)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `mercury_transaction_id`
  ON `bank_transaction` (`mercury_transaction_id` ASC);

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `bank_transaction` (`id` ASC);

CREATE INDEX `fk_xero_bills_idx`
  ON `bank_transaction` (`xero_bill_id` ASC);

CREATE INDEX `fk_xero_spend_money_idx`
  ON `bank_transaction` (`xero_spend_money_id` ASC);

-- -----------------------------------------------------
-- Table: project
-- -----------------------------------------------------
DROP TABLE IF EXISTS `project`;
CREATE TABLE IF NOT EXISTS `project` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_number` INT NULL DEFAULT NULL,
  `name` VARCHAR(100) NOT NULL,
  `status` ENUM('Active','Closed') NOT NULL DEFAULT 'Active',
  `total_spent` DECIMAL(10,2) NULL DEFAULT '0.00',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
)
ENGINE = InnoDB
AUTO_INCREMENT = 3
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `project` (`id` ASC);

-- -----------------------------------------------------
-- Table: contact
-- -----------------------------------------------------
DROP TABLE IF EXISTS `contact`;
CREATE TABLE IF NOT EXISTS `contact` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `pulse_id` BIGINT NULL DEFAULT NULL,
  `name` VARCHAR(255) NOT NULL,
  `vendor_type` VARCHAR(45) NULL DEFAULT NULL,
  `payment_details` VARCHAR(255) NOT NULL DEFAULT 'PENDING',
  `email` VARCHAR(100) NULL DEFAULT NULL,
  `phone` VARCHAR(45) NULL DEFAULT NULL,
  `address_line_1` VARCHAR(255) NULL DEFAULT NULL,
  `city` VARCHAR(100) NULL DEFAULT NULL,
  `zip` VARCHAR(20) NULL DEFAULT NULL,
  `tax_form_link` VARCHAR(255) NULL DEFAULT NULL,
  `vendor_status` ENUM('PENDING','TO VERIFY','APPROVED','ISSUE') NOT NULL DEFAULT 'PENDING',
  `country` VARCHAR(100) NULL DEFAULT NULL,
  `tax_type` VARCHAR(45) NULL DEFAULT 'SSN',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `tax_number` BIGINT NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
)
ENGINE = InnoDB
AUTO_INCREMENT = 73
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `contact` (`id` ASC);

CREATE UNIQUE INDEX `pulse_id`
  ON `contact` (`pulse_id` ASC);

-- -----------------------------------------------------
-- Table: purchase_order
--    (with new contact_id to form a one-to-many: one Contact, many POs)
-- -----------------------------------------------------
DROP TABLE IF EXISTS `purchase_order`;
CREATE TABLE IF NOT EXISTS `purchase_order` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_id` INT UNSIGNED NOT NULL,
  `po_number` INT UNSIGNED NOT NULL,
  `description` VARCHAR(255) NULL DEFAULT NULL,
  `po_type` VARCHAR(45) NULL DEFAULT NULL,
  `state` ENUM('APPROVED','TO VERIFY','ISSUE','PENDING','CC / PC') NOT NULL DEFAULT 'PENDING',
  `amount_total` DECIMAL(15,2) NOT NULL DEFAULT '0.00',
  `producer` VARCHAR(100) NULL DEFAULT NULL,
  `tax_form_link` VARCHAR(255) NULL DEFAULT NULL,
  `pulse_id` BIGINT NULL DEFAULT NULL,
  `folder_link` VARCHAR(255) NULL DEFAULT NULL,
  `contact_id` INT UNSIGNED NULL DEFAULT NULL, -- NEW COLUMN FOR 1-to-MANY RELATIONSHIP
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_purchase_order_project`
    FOREIGN KEY (`project_id`)
    REFERENCES `project` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `fk_purchase_order_contact`
    FOREIGN KEY (`contact_id`)
    REFERENCES `contact` (`id`)
    ON DELETE SET NULL
)
ENGINE = InnoDB
AUTO_INCREMENT = 161
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `project_id`
  ON `purchase_order` (`project_id` ASC, `po_number` ASC);

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `purchase_order` (`id` ASC);

CREATE INDEX `project_id_2`
  ON `purchase_order` (`project_id` ASC);

-- -----------------------------------------------------
-- Table: detail_item
-- -----------------------------------------------------
DROP TABLE IF EXISTS `detail_item`;
CREATE TABLE IF NOT EXISTS `detail_item` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `po_id` INT UNSIGNED NOT NULL,
  `state` ENUM('PENDING','OVERDUE','ISSUE','RTP','RECONCILED','PAID','APPROVED') NOT NULL DEFAULT 'PENDING',
  `detail_number` INT UNSIGNED NOT NULL,
  `line_id` INT UNSIGNED NOT NULL,
  `aicp_code` INT UNSIGNED NOT NULL,
  `vendor` VARCHAR(255) NULL DEFAULT NULL,
  `description` VARCHAR(255) NULL DEFAULT NULL,
  `transaction_date` DATETIME NULL DEFAULT NULL,
  `due_date` DATETIME NULL DEFAULT NULL,
  `rate` DECIMAL(15,2) NOT NULL,
  `quantity` DECIMAL(15,2) NOT NULL DEFAULT '1.00',
  `ot` DECIMAL(15,2) NULL DEFAULT '0.00',
  `fringes` DECIMAL(15,2) NULL DEFAULT '0.00',
  `sub_total` DECIMAL(15,2) GENERATED ALWAYS AS (
       round(((rate * quantity) + IFNULL(ot,0) + IFNULL(fringes,0)),2)
    ) STORED,
  `file_link` VARCHAR(255) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_detail_item_aicp_code`
    FOREIGN KEY (`aicp_code`)
    REFERENCES `aicp_code` (`id`),
  CONSTRAINT `fk_detail_item_purchase_order`
    FOREIGN KEY (`po_id`)
    REFERENCES `purchase_order` (`id`)
    ON DELETE CASCADE
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `detail_item` (`id` ASC);

CREATE INDEX `po_id`
  ON `detail_item` (`po_id` ASC);

CREATE INDEX `fk_aicp_code_idx`
  ON `detail_item` (aicp_code_id ASC);

-- -----------------------------------------------------
-- Table: bill_line_item
-- -----------------------------------------------------
DROP TABLE IF EXISTS `bill_line_item`;
CREATE TABLE IF NOT EXISTS `bill_line_item` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `xero_bill_id` INT UNSIGNED NOT NULL,
  `detail_item_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`, `detail_item_id`, `xero_bill_id`),
  CONSTRAINT `fk_bill_line_item_detail_item`
    FOREIGN KEY (`detail_item_id`)
    REFERENCES `detail_item` (`id`),
  CONSTRAINT `fk_bill_line_item_xero_bill`
    FOREIGN KEY (`xero_bill_id`)
    REFERENCES `xero_bill` (`id`)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `bill_line_item` (`id` ASC);

CREATE INDEX `fk_detail_item_idx`
  ON `bill_line_item` (`detail_item_id` ASC);

CREATE INDEX `fk_xero_bill_idx`
  ON `bill_line_item` (`xero_bill_id` ASC);

-- -----------------------------------------------------
-- Table: invoice
-- -----------------------------------------------------
DROP TABLE IF EXISTS `invoice`;
CREATE TABLE IF NOT EXISTS `invoice` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `transaction_date` DATETIME NULL DEFAULT NULL,
  `term` INT NULL DEFAULT NULL,
  `total` DECIMAL(15,2) NULL DEFAULT '0.00',
  `file_link` VARCHAR(255) NULL DEFAULT NULL,
  `po_id` INT UNSIGNED NOT NULL,
  `project_id` INT UNSIGNED NOT NULL,
  `invoice_number` INT UNSIGNED NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_invoice_purchase_order`
    FOREIGN KEY (`po_id`)
    REFERENCES `purchase_order` (`id`)
    ON DELETE CASCADE
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `invoice` (`id` ASC);

CREATE INDEX `po_id`
  ON `invoice` (`po_id` ASC);

-- -----------------------------------------------------
-- Table: receipt
-- -----------------------------------------------------
DROP TABLE IF EXISTS `receipt`;
CREATE TABLE IF NOT EXISTS `receipt` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `total` DECIMAL(15,2) NULL DEFAULT '0.00',
  `receipt_description` VARCHAR(45) NULL DEFAULT NULL,
  `purchase_date` DATETIME NULL DEFAULT NULL,
  `file_link` VARCHAR(255) NULL DEFAULT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `spend_money_id` INT UNSIGNED NOT NULL,
  `detail_item_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`, `detail_item_id`),
  CONSTRAINT `fk_receipt_detail_item`
    FOREIGN KEY (`detail_item_id`)
    REFERENCES `detail_item` (`id`),
  CONSTRAINT `fk_receipt_spend_money`
    FOREIGN KEY (`spend_money_id`)
    REFERENCES `spend_money` (`id`)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_unicode_ci;

CREATE UNIQUE INDEX `id_UNIQUE`
  ON `receipt` (`id` ASC);

CREATE INDEX `fk_spend_money_idx`
  ON `receipt` (`spend_money_id` ASC);

CREATE INDEX `fk_detail_item_idx`
  ON `receipt` (`detail_item_id` ASC);

-- -----------------------------------------------------
-- Example placeholders for views 
-- -----------------------------------------------------
DROP TABLE IF EXISTS `vw_average_po_value_per_project`;
CREATE TABLE IF NOT EXISTS `vw_average_po_value_per_project` (`project_id` INT, `project_name` INT, `avg_po_value` INT);

DROP TABLE IF EXISTS `vw_detail_items_summary`;
CREATE TABLE IF NOT EXISTS `vw_detail_items_summary` (`po_id` INT, `project_name` INT, `po_number` INT, `total_items` INT, `total_sub_total` INT, `avg_item_cost` INT);

DROP TABLE IF EXISTS `vw_invoice_summary`;
CREATE TABLE IF NOT EXISTS `vw_invoice_summary` (`invoice_id` INT, `invoice_number` INT, `invoice_total` INT, `transaction_date` INT, `project_name` INT, `po_number` INT, `created_at` INT, `updated_at` INT);

DROP TABLE IF EXISTS `vw_project_totals`;
CREATE TABLE IF NOT EXISTS `vw_project_totals` (`project_id` INT, `project_name` INT, `status` INT, `total_spent` INT, `number_of_purchase_orders` INT);

DROP TABLE IF EXISTS `vw_purchase_order_details`;
CREATE TABLE IF NOT EXISTS `vw_purchase_order_details` (`po_id` INT, `project_name` INT, `po_number` INT, `amount_total` INT, `contact_name` INT, `state` INT, `created_at` INT, `updated_at` INT);

-- -----------------------------------------------------
-- sp_create_investor_views (updated to reference PO.contact_id)
-- -----------------------------------------------------
USE `virtual_pm`;
DROP PROCEDURE IF EXISTS `sp_create_investor_views`;
DELIMITER $$
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
END $$
DELIMITER ;

USE `virtual_pm`;

-- -----------------------------------------------------
-- Triggers remain unchanged
-- -----------------------------------------------------

-- AFTER DELETE on purchase_order
DROP TRIGGER IF EXISTS `trg_purchase_order_ad_update_project`;
CREATE
DEFINER=`root`@`localhost`
TRIGGER `trg_purchase_order_ad_update_project`
AFTER DELETE
ON `purchase_order`
FOR EACH ROW
BEGIN
    DECLARE project_total DECIMAL(15,2);
    SELECT IFNULL(SUM(amount_total), 0.00)
      INTO project_total
      FROM purchase_order
      WHERE project_id = OLD.project_id;

    UPDATE project
    SET total_spent = project_total
    WHERE id = OLD.project_id;
END;

-- AFTER UPDATE on purchase_order
DROP TRIGGER IF EXISTS `trg_purchase_order_au_update_project`;
CREATE
DEFINER=`root`@`localhost`
TRIGGER `trg_purchase_order_au_update_project`
AFTER UPDATE
ON `purchase_order`
FOR EACH ROW
BEGIN
    DECLARE project_total DECIMAL(15,2);
    IF OLD.amount_total <> NEW.amount_total OR OLD.project_id <> NEW.project_id THEN
        SELECT IFNULL(SUM(amount_total), 0.00)
            INTO project_total
            FROM purchase_order
            WHERE project_id = NEW.project_id;

        UPDATE project
        SET total_spent = project_total
        WHERE id = NEW.project_id;
    END IF;
END;

-- AFTER DELETE on detail_item
DROP TRIGGER IF EXISTS `trg_detail_item_ad_update_po_total`;
CREATE
DEFINER=`root`@`localhost`
TRIGGER `trg_detail_item_ad_update_po_total`
AFTER DELETE
ON `detail_item`
FOR EACH ROW
BEGIN
    DECLARE total DECIMAL(15,2);
    SELECT IFNULL(SUM(sub_total), 0.00)
      INTO total
      FROM detail_item
      WHERE po_id = OLD.po_id;

    UPDATE purchase_order
    SET amount_total = total
    WHERE id = OLD.po_id;
END;

-- AFTER INSERT on detail_item
DROP TRIGGER IF EXISTS `trg_detail_item_ai_update_po_total`;
CREATE
DEFINER=`root`@`localhost`
TRIGGER `trg_detail_item_ai_update_po_total`
AFTER INSERT
ON `detail_item`
FOR EACH ROW
BEGIN
    DECLARE total DECIMAL(15,2);
    SELECT IFNULL(SUM(sub_total), 0.00)
      INTO total
      FROM detail_item
      WHERE po_id = NEW.po_id;

    UPDATE purchase_order
    SET amount_total = total
    WHERE id = NEW.po_id;
END;

-- AFTER UPDATE on detail_item
DROP TRIGGER IF EXISTS `trg_detail_item_au_update_po_total`;
CREATE
DEFINER=`root`@`localhost`
TRIGGER `trg_detail_item_au_update_po_total`
AFTER UPDATE
ON `detail_item`
FOR EACH ROW
BEGIN
    DECLARE total DECIMAL(15,2);
    SELECT IFNULL(SUM(sub_total), 0.00)
      INTO total
      FROM detail_item
      WHERE po_id = NEW.po_id;

    UPDATE purchase_order
    SET amount_total = total
    WHERE id = NEW.po_id;
END;

-- Restore configs
SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;