create definer = root@localhost trigger account_code_ad
    after delete
    on account_code
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'account_code'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted account_code.id=', OLD.id)
    );
END;

