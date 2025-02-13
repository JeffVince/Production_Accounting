create definer = root@localhost trigger purchase_order_au
    after update
    on purchase_order
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'purchase_order'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated purchase_order.id=', NEW.id)
    );
END;

