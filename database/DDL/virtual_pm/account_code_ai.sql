create definer = root@localhost trigger account_code_ai
    after insert
    on account_code
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'account_code'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted account_code.id=', NEW.id)
    );
END;

