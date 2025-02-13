create definer = root@localhost trigger invoice_au
    after update
    on invoice
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'invoice'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated invoice.id=', NEW.id)
    );
END;

