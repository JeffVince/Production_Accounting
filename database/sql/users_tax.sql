-- =======================================================
-- 1) Drop any existing tables (if they exist) to avoid duplicates
--    (NOTE: Adjust 'CASCADE' or reference options per your DB needs)
-- =======================================================
DROP TABLE IF EXISTS `users`;
DROP TABLE IF EXISTS `tax_ledger`;
DROP TABLE IF EXISTS `budget_map`;

-- If you have existing references in `tax_account` or `account_code`,
-- you may need to drop or modify constraints first. Example:


-- =======================================================
-- 2) Create new tables: users, tax_ledger, budget_map
-- =======================================================

CREATE TABLE IF NOT EXISTS `users` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `username` VARCHAR(100) NOT NULL,
    `contact_id` INT UNSIGNED NULL,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    CONSTRAINT `fk_user_contact`
        FOREIGN KEY (`contact_id`)
        REFERENCES `contact`(`id`)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS `tax_ledger` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(100) NOT NULL,
    `user_id` INT UNSIGNED NOT NULL,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    CONSTRAINT `fk_tax_ledger_users`
        FOREIGN KEY (`user_id`)
        REFERENCES `users`(`id`)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS `budget_map` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `map_name` VARCHAR(100) NOT NULL,
    `user_id` INT UNSIGNED NOT NULL,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    CONSTRAINT `fk_budget_map_users`
        FOREIGN KEY (`user_id`)
        REFERENCES `users`(`id`)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- =======================================================
-- 3) Modify existing tables: tax_account, account_code
--    Add new foreign key columns & remove old columns
-- =======================================================

-- a) Add tax_ledger_id to tax_account
ALTER TABLE `tax_account`
  ADD COLUMN `tax_ledger_id` INT UNSIGNED NULL DEFAULT NULL AFTER `description`;

-- b) Create a foreign key constraint for the new column
ALTER TABLE `tax_account`
  ADD CONSTRAINT `fk_tax_account_tax_ledger`
    FOREIGN KEY (`tax_ledger_id`)
    REFERENCES `tax_ledger`(`id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE;

-- c) In account_code, remove map_code and add budget_map_id
ALTER TABLE `account_code`
  DROP COLUMN `map_code`,
  ADD COLUMN `budget_map_id` INT UNSIGNED NULL DEFAULT NULL AFTER `code`;

ALTER TABLE `account_code`
  ADD CONSTRAINT `fk_account_code_budget_map`
    FOREIGN KEY (`budget_map_id`)
    REFERENCES `budget_map`(`id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE;

-- =======================================================
-- 4) Add triggers to log changes (INSERT, UPDATE, DELETE)
--    for each new table. Adjust to your naming conventions.
-- =======================================================

-- ------------------------
-- USERS TABLE TRIGGERS
-- ------------------------
DELIMITER $$

CREATE TRIGGER `users_ai`
AFTER INSERT ON `users`
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, message)
    VALUES ('users', 'INSERT', NEW.id, CONCAT('Inserted users.id=', NEW.id));
END$$

CREATE TRIGGER `users_au`
AFTER UPDATE ON `users`
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, message)
    VALUES ('users', 'UPDATE', NEW.id, CONCAT('Updated users.id=', NEW.id));
END$$

CREATE TRIGGER `users_ad`
AFTER DELETE ON `users`
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, message)
    VALUES ('users', 'DELETE', OLD.id, CONCAT('Deleted users.id=', OLD.id));
END$$

-- ------------------------
-- TAX_LEDGER TABLE TRIGGERS
-- ------------------------
CREATE TRIGGER `tax_ledger_ai`
AFTER INSERT ON `tax_ledger`
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, message)
    VALUES ('tax_ledger', 'INSERT', NEW.id, CONCAT('Inserted tax_ledger.id=', NEW.id));
END$$

CREATE TRIGGER `tax_ledger_au`
AFTER UPDATE ON `tax_ledger`
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, message)
    VALUES ('tax_ledger', 'UPDATE', NEW.id, CONCAT('Updated tax_ledger.id=', NEW.id));
END$$

CREATE TRIGGER `tax_ledger_ad`
AFTER DELETE ON `tax_ledger`
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, message)
    VALUES ('tax_ledger', 'DELETE', OLD.id, CONCAT('Deleted tax_ledger.id=', OLD.id));
END$$

-- ------------------------
-- BUDGET_MAP TABLE TRIGGERS
-- ------------------------
CREATE TRIGGER `budget_map_ai`
AFTER INSERT ON `budget_map`
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, message)
    VALUES ('budget_map', 'INSERT', NEW.id, CONCAT('Inserted budget_map.id=', NEW.id));
END$$

CREATE TRIGGER `budget_map_au`
AFTER UPDATE ON `budget_map`
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, message)
    VALUES ('budget_map', 'UPDATE', NEW.id, CONCAT('Updated budget_map.id=', NEW.id));
END$$

CREATE TRIGGER `budget_map_ad`
AFTER DELETE ON `budget_map`
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, operation, record_id, message)
    VALUES ('budget_map', 'DELETE', OLD.id, CONCAT('Deleted budget_map.id=', OLD.id));
END$$

DELIMITER ;

-- End of SQL changes