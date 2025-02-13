create definer = root@localhost trigger tax_ledger_ad
    after delete
    on tax_ledger
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_ledger'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted tax_ledger.id=', OLD.id)
    );
END;

