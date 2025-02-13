create definer = root@localhost trigger spend_money_ad
    after delete
    on spend_money
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'spend_money'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted spend_money.id=', OLD.id)
    );
END;

