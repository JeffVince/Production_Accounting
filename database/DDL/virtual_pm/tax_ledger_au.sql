create definer = root@localhost trigger tax_ledger_au
    after update
    on tax_ledger
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_ledger'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated tax_ledger.id=', NEW.id)
    );
END;

