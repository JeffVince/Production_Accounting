create definer = root@localhost trigger users_au
    after update
    on users
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'users'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated users.id=', NEW.id)
    );
END;

