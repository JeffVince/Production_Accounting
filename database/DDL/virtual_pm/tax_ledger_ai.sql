create definer = root@localhost trigger tax_ledger_ai
    after insert
    on tax_ledger
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_ledger'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted tax_ledger.id=', NEW.id)
    );
END;

