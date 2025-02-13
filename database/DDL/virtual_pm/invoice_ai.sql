create definer = root@localhost trigger invoice_ai
    after insert
    on invoice
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'invoice'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted invoice.id=', NEW.id)
    );
END;

