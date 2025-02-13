create definer = root@localhost trigger bank_transaction_ai
    after insert
    on bank_transaction
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'bank_transaction'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted bank_transaction.id=', NEW.id)
    );
END;

