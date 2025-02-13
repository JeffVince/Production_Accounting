create definer = root@localhost trigger users_ad
    after delete
    on users
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'users'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted users.id=', OLD.id)
    );
END;

