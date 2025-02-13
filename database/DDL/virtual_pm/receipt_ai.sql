create definer = root@localhost trigger receipt_ai
    after insert
    on receipt
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'receipt'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted receipt.id=', NEW.id)
    );
END;

