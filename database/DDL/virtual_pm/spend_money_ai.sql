create definer = root@localhost trigger spend_money_ai
    after insert
    on spend_money
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'spend_money'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted spend_money.id=', NEW.id)
    );
END;

