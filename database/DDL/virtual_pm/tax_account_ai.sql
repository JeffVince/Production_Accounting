create definer = root@localhost trigger tax_account_ai
    after insert
    on tax_account
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_account'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted tax_account.id=', NEW.id)
    );
END;

