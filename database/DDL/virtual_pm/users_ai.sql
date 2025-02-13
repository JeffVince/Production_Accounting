create definer = root@localhost trigger users_ai
    after insert
    on users
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'users'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted users.id=', NEW.id)
    );
END;

