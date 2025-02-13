create definer = root@localhost trigger bank_transaction_au
    after update
    on bank_transaction
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'bank_transaction'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated bank_transaction.id=', NEW.id)
    );
END;

