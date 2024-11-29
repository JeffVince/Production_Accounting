INSERT INTO tax_accounts (tax_code_id, code, description)
VALUES
('5000', 1, 'Description for 5000'),
('5300', 2, 'Description for 5300'),
('6040', 3, 'Description for 6040'),
('5330', 4, 'Description for 5330');


INSERT INTO aicp_codes (aicp_code_surrogate_id, code, tax_code_id, description)
VALUES
(1,'5000', '5000', 'Description for 5000'),
(2,'5300', '5300', 'Description for 5300'),
(3,'6040', '6040', 'Description for 6040'),
(4,'5330', '5330', 'Description for 5330');



INSERT INTO projects (project_id, name, status)
VALUES
('2417', 'Whop Creator Profiles', 'Active'),
('2416', 'Whop Keynote', 'Active'),
('2419', 'The Crowd', 'Active');

DELIMITER $$

DROP TRIGGER IF EXISTS `virtual_pm`.`detail_items_BEFORE_INSERT`$$

CREATE DEFINER = CURRENT_USER TRIGGER `virtual_pm`.`detail_items_BEFORE_INSERT`
BEFORE INSERT ON `detail_items`
FOR EACH ROW
BEGIN
    DECLARE parent_po_type VARCHAR(45);

    -- Retrieve the po_type from the parent purchase order
    SELECT `po_type` INTO parent_po_type
    FROM `virtual_pm`.`purchase_orders`
    WHERE `po_surrogate_id` = NEW.`parent_id`
    LIMIT 1;
    -- Set is_receipt based on po_type
    IF parent_po_type = 'Vendor' THEN
        SET NEW.`is_receipt` = 0; -- Bill/Invoice
    ELSE
        SET NEW.`is_receipt` = 1; -- Receipt
    END IF;
    -- Set due_date based on is_receipt
    IF NEW.`is_receipt` = 1 THEN
        -- If it is a receipt, due_date is the same as transaction_date
        SET NEW.`due_date` = NEW.`transaction_date`;
    ELSE
        -- Otherwise, due_date is 30 days after transaction_date
        SET NEW.`due_date` = DATE_ADD(NEW.`transaction_date`, INTERVAL 30 DAY);
    END IF;
END$$
DELIMITER ;




DELIMITER $$

CREATE DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`detail_items_AFTER_INSERT_update_po_total`
AFTER INSERT ON `virtual_pm`.`detail_items`
FOR EACH ROW
BEGIN
    DECLARE total DECIMAL(15,2);

    -- Calculate the sum of sub_totals for the parent purchase order
    SELECT SUM(`sub_total`) INTO total
    FROM `virtual_pm`.`detail_items`
    WHERE `parent_id` = NEW.`parent_id`;

    -- Update the amount_total in purchase_orders
    UPDATE `virtual_pm`.`purchase_orders`
    SET `amount_total` = IFNULL(total, 0.00)
    WHERE `po_surrogate_id` = NEW.`parent_id`;
END$$

DELIMITER ;


DELIMITER $$

CREATE DEFINER=`root`@`localhost`
TRIGGER `virtual_pm`.`detail_items_AFTER_UPDATE_update_po_total`
AFTER UPDATE ON `virtual_pm`.`detail_items`
FOR EACH ROW
BEGIN
    DECLARE total DECIMAL(15,2);

    -- Calculate the sum of sub_totals for the parent purchase order
    SELECT SUM(`sub_total`) INTO total
    FROM `virtual_pm`.`detail_items`
    WHERE `parent_id` = NEW.`parent_id`;

    -- Update the amount_total in purchase_orders
    UPDATE `virtual_pm`.`purchase_orders`
    SET `amount_total` = IFNULL(total, 0.00)
    WHERE `po_surrogate_id` = NEW.`parent_id`;
END$$

DELIMITER ;


DROP VIEW IF EXISTS `virtual_pm`.`vw_purchase_order_summary`;

CREATE ALGORITHM = UNDEFINED
DEFINER = `root`@`localhost`
SQL SECURITY DEFINER
VIEW `virtual_pm`.`vw_purchase_order_summary` AS
SELECT
    CASE
        WHEN po.`state` = 'CC / PC' THEN po.description
        ELSE c.`name`
    END AS `Contact Name`,
    p.`project_id` AS `Project ID`,
    po.`po_number` AS `PO #`,
    po.`description` AS `Description`,
    CASE
        WHEN po.`tax_form_link` IS NOT NULL AND po.`tax_form_link` != '' THEN 'Yes'
        ELSE 'No'
    END AS `Tax Form Link Exists`,
    po.`amount_total` AS `Total Amount`,
    po.`state` AS `PO Status`,
    CASE
        WHEN po.`folder_link` IS NOT NULL AND po.`folder_link` != '' THEN 'Yes'
        ELSE 'No'
    END AS `Folder Link Exists`,

    c.`email` AS `Contact Email`,
    c.`phone` AS `Contact Phone`,
    c.`tax_ID` AS `Contact SSN`
FROM
    `virtual_pm`.`purchase_orders` po
    INNER JOIN `virtual_pm`.`projects` p
        ON po.`project_id` = p.`project_id`
    LEFT JOIN `virtual_pm`.`contacts` c
        ON po.`contact_id` = c.`pulse_id`;


DROP VIEW IF EXISTS `virtual_pm`.`vw_purchase_order_details`;

CREATE ALGORITHM = UNDEFINED
DEFINER = `root`@`localhost`
SQL SECURITY DEFINER
VIEW `virtual_pm`.`vw_purchase_order_details` AS
SELECT
    CASE
        WHEN po.`state` = 'CC / PC' THEN po.description
        ELSE c.`name`
    END AS `Contact Name`,
    p.`project_id` AS `Project ID`,
    po.`po_number` AS `PO #`,
    di.`detail_item_number` AS `Detail Item Number`,
    po.`description` AS `Description`,
    CASE
        WHEN po.`tax_form_link` IS NOT NULL AND po.`tax_form_link` != '' THEN 'Yes'
        ELSE 'No'
    END AS `Tax Form Link Exists`,
    di.`sub_total` AS `Detail Item Subtotal`,
    po.`amount_total` AS `Total Amount`,
    po.`state` AS `PO Status`,
    CASE
        WHEN po.`folder_link` IS NOT NULL AND po.`folder_link` != '' THEN 'Yes'
        ELSE 'No'
    END AS `Folder Link Exists`,
    CASE
        WHEN c.`vendor_type` = 'CC / PC' THEN 'CREDIT CARD'
        ELSE c.`vendor_type`
    END AS `Vendor Type`,
    c.`email` AS `Contact Email`,
    c.`phone` AS `Contact Phone`,
    c.`tax_ID` AS `Contact SSN`
FROM
    `virtual_pm`.`detail_items` di
    INNER JOIN `virtual_pm`.`purchase_orders` po
        ON di.`parent_id` = po.`po_surrogate_id`
    INNER JOIN `virtual_pm`.`projects` p
        ON po.`project_id` = p.`project_id`
    LEFT JOIN `virtual_pm`.`contacts` c
        ON po.`contact_id` = c.`pulse_id`;