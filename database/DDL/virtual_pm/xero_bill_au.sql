create definer = root@localhost trigger xero_bill_au
    after update
    on xero_bill
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'xero_bill'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated xero_bill.id=', NEW.id)
    );
END;

