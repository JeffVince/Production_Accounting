USE `virtual_pm`;

-----------------------------------------------------
-- 1) Create a simple audit_log table (optional)
-----------------------------------------------------
-- This is just an example table to illustrate storing trigger events.
-- If you already have an audit or logging mechanism, skip or adapt as needed.

DROP TABLE IF EXISTS `audit_log`;
CREATE TABLE `audit_log` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `table_name` VARCHAR(100) NOT NULL,
  `operation` VARCHAR(10) NOT NULL,   -- 'INSERT','UPDATE','DELETE'
  `record_id` INT UNSIGNED NULL,
  `message` VARCHAR(255) NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DELIMITER $$

-----------------------------------------------------
-- 2) TAX_ACCOUNT triggers (insert, update, delete)
-----------------------------------------------------

CREATE TRIGGER `tax_account_ai`
AFTER INSERT ON `tax_account`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('tax_account', 'INSERT', NEW.id, CONCAT('Inserted tax_account.id=', NEW.id));
END $$

CREATE TRIGGER `tax_account_au`
AFTER UPDATE ON `tax_account`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('tax_account', 'UPDATE', NEW.id, CONCAT('Updated tax_account.id=', NEW.id));
END $$

CREATE TRIGGER `tax_account_ad`
AFTER DELETE ON `tax_account`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('tax_account', 'DELETE', OLD.id, CONCAT('Deleted tax_account.id=', OLD.id));
END $$

-----------------------------------------------------
-- 3) AICP_CODE triggers
-----------------------------------------------------

CREATE TRIGGER `aicp_code_ai`
AFTER INSERT ON account_code
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('aicp_code', 'INSERT', NEW.id, CONCAT('Inserted aicp_code.id=', NEW.id));
END $$

CREATE TRIGGER `aicp_code_au`
AFTER UPDATE ON account_code
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('aicp_code', 'UPDATE', NEW.id, CONCAT('Updated aicp_code.id=', NEW.id));
END $$

CREATE TRIGGER `aicp_code_ad`
AFTER DELETE ON account_code
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('aicp_code', 'DELETE', OLD.id, CONCAT('Deleted aicp_code.id=', OLD.id));
END $$

-----------------------------------------------------
-- 4) XERO_BILL triggers
-----------------------------------------------------

CREATE TRIGGER `xero_bill_ai`
AFTER INSERT ON `xero_bill`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('xero_bill', 'INSERT', NEW.id, CONCAT('Inserted xero_bill.id=', NEW.id));
END $$

CREATE TRIGGER `xero_bill_au`
AFTER UPDATE ON `xero_bill`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('xero_bill', 'UPDATE', NEW.id, CONCAT('Updated xero_bill.id=', NEW.id));
END $$

CREATE TRIGGER `xero_bill_ad`
AFTER DELETE ON `xero_bill`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('xero_bill', 'DELETE', OLD.id, CONCAT('Deleted xero_bill.id=', OLD.id));
END $$

-----------------------------------------------------
-- 5) SPEND_MONEY triggers
-----------------------------------------------------

CREATE TRIGGER `spend_money_ai`
AFTER INSERT ON `spend_money`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('spend_money', 'INSERT', NEW.id, CONCAT('Inserted spend_money.id=', NEW.id));
END $$

CREATE TRIGGER `spend_money_au`
AFTER UPDATE ON `spend_money`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('spend_money', 'UPDATE', NEW.id, CONCAT('Updated spend_money.id=', NEW.id));
END $$

CREATE TRIGGER `spend_money_ad`
AFTER DELETE ON `spend_money`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('spend_money', 'DELETE', OLD.id, CONCAT('Deleted spend_money.id=', OLD.id));
END $$

-----------------------------------------------------
-- 6) BANK_TRANSACTION triggers
-----------------------------------------------------

CREATE TRIGGER `bank_transaction_ai`
AFTER INSERT ON `bank_transaction`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('bank_transaction', 'INSERT', NEW.id, CONCAT('Inserted bank_transaction.id=', NEW.id));
END $$

CREATE TRIGGER `bank_transaction_au`
AFTER UPDATE ON `bank_transaction`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('bank_transaction', 'UPDATE', NEW.id, CONCAT('Updated bank_transaction.id=', NEW.id));
END $$

CREATE TRIGGER `bank_transaction_ad`
AFTER DELETE ON `bank_transaction`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('bank_transaction', 'DELETE', OLD.id, CONCAT('Deleted bank_transaction.id=', OLD.id));
END $$

-----------------------------------------------------
-- 7) BILL_LINE_ITEM triggers
-----------------------------------------------------

CREATE TRIGGER `bill_line_item_ai`
AFTER INSERT ON `bill_line_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('bill_line_item', 'INSERT', NEW.id, CONCAT('Inserted bill_line_item.id=', NEW.id));
END $$

CREATE TRIGGER `bill_line_item_au`
AFTER UPDATE ON `bill_line_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('bill_line_item', 'UPDATE', NEW.id, CONCAT('Updated bill_line_item.id=', NEW.id));
END $$

CREATE TRIGGER `bill_line_item_ad`
AFTER DELETE ON `bill_line_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('bill_line_item', 'DELETE', OLD.id, CONCAT('Deleted bill_line_item.id=', OLD.id));
END $$

-----------------------------------------------------
-- 8) CONTACT triggers
-----------------------------------------------------

CREATE TRIGGER `contact_ai`
AFTER INSERT ON `contact`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('contact', 'INSERT', NEW.id, CONCAT('Inserted contact.id=', NEW.id));
END $$

CREATE TRIGGER `contact_au`
AFTER UPDATE ON `contact`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('contact', 'UPDATE', NEW.id, CONCAT('Updated contact.id=', NEW.id));
END $$

CREATE TRIGGER `contact_ad`
AFTER DELETE ON `contact`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('contact', 'DELETE', OLD.id, CONCAT('Deleted contact.id=', OLD.id));
END $$

-----------------------------------------------------
-- 9) PURCHASE_ORDER triggers
-----------------------------------------------------
-- (Note: you already have some triggers that update project totals,
--  so these "audit" triggers won't conflict as long as the names differ.)

CREATE TRIGGER `purchase_order_ai`
AFTER INSERT ON `purchase_order`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('purchase_order', 'INSERT', NEW.id, CONCAT('Inserted purchase_order.id=', NEW.id));
END $$

CREATE TRIGGER `purchase_order_au`
AFTER UPDATE ON `purchase_order`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('purchase_order', 'UPDATE', NEW.id, CONCAT('Updated purchase_order.id=', NEW.id));
END $$

CREATE TRIGGER `purchase_order_ad`
AFTER DELETE ON `purchase_order`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('purchase_order', 'DELETE', OLD.id, CONCAT('Deleted purchase_order.id=', OLD.id));
END $$

-----------------------------------------------------
-- 10) DETAIL_ITEM triggers
-----------------------------------------------------
-- (Again, you already have triggers that recalc POs, so these are purely example.)

CREATE TRIGGER `detail_item_ai`
AFTER INSERT ON `detail_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('detail_item', 'INSERT', NEW.id, CONCAT('Inserted detail_item.id=', NEW.id));
END $$

CREATE TRIGGER `detail_item_au`
AFTER UPDATE ON `detail_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('detail_item', 'UPDATE', NEW.id, CONCAT('Updated detail_item.id=', NEW.id));
END $$

CREATE TRIGGER `detail_item_ad`
AFTER DELETE ON `detail_item`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('detail_item', 'DELETE', OLD.id, CONCAT('Deleted detail_item.id=', OLD.id));
END $$

-----------------------------------------------------
-- 11) INVOICE triggers
-----------------------------------------------------

CREATE TRIGGER `invoice_ai`
AFTER INSERT ON `invoice`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('invoice', 'INSERT', NEW.id, CONCAT('Inserted invoice.id=', NEW.id));
END $$

CREATE TRIGGER `invoice_au`
AFTER UPDATE ON `invoice`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('invoice', 'UPDATE', NEW.id, CONCAT('Updated invoice.id=', NEW.id));
END $$

CREATE TRIGGER `invoice_ad`
AFTER DELETE ON `invoice`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('invoice', 'DELETE', OLD.id, CONCAT('Deleted invoice.id=', OLD.id));
END $$

-----------------------------------------------------
-- 12) PROJECT triggers
-----------------------------------------------------

CREATE TRIGGER `project_ai`
AFTER INSERT ON `project`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('project', 'INSERT', NEW.id, CONCAT('Inserted project.id=', NEW.id));
END $$

CREATE TRIGGER `project_au`
AFTER UPDATE ON `project`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('project', 'UPDATE', NEW.id, CONCAT('Updated project.id=', NEW.id));
END $$

CREATE TRIGGER `project_ad`
AFTER DELETE ON `project`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('project', 'DELETE', OLD.id, CONCAT('Deleted project.id=', OLD.id));
END $$

-----------------------------------------------------
-- 13) RECEIPT triggers
-----------------------------------------------------

CREATE TRIGGER `receipt_ai`
AFTER INSERT ON `receipt`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('receipt', 'INSERT', NEW.id, CONCAT('Inserted receipt.id=', NEW.id));
END $$

CREATE TRIGGER `receipt_au`
AFTER UPDATE ON `receipt`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('receipt', 'UPDATE', NEW.id, CONCAT('Updated receipt.id=', NEW.id));
END $$

CREATE TRIGGER `receipt_ad`
AFTER DELETE ON `receipt`
FOR EACH ROW
BEGIN
    INSERT INTO `audit_log` (table_name, operation, record_id, message)
    VALUES ('receipt', 'DELETE', OLD.id, CONCAT('Deleted receipt.id=', OLD.id));
END $$

DELIMITER ;

-- Done!
-- At this point, each table in `virtual_pm` has 3 triggers (insert/update/delete).
-- This is just an example for logging. Adapt as needed for your actual trigger logic.