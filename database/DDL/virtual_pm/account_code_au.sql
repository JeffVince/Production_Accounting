create definer = root@localhost trigger account_code_au
    after update
    on account_code
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'account_code'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated account_code.id=', NEW.id)
    );
END;

