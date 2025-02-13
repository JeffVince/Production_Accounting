create definer = root@localhost trigger tax_account_au
    after update
    on tax_account
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_account'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated tax_account.id=', NEW.id)
    );
END;

