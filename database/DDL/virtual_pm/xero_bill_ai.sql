create definer = root@localhost trigger xero_bill_ai
    after insert
    on xero_bill
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'xero_bill'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted xero_bill.id=', NEW.id)
    );
END;

