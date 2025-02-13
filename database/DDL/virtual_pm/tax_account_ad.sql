create definer = root@localhost trigger tax_account_ad
    after delete
    on tax_account
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_account'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted tax_account.id=', OLD.id)
    );
END;

