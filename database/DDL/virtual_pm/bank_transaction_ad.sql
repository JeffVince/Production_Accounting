create definer = root@localhost trigger bank_transaction_ad
    after delete
    on bank_transaction
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'bank_transaction'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted bank_transaction.id=', OLD.id)
    );
END;

