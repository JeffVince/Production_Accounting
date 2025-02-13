create definer = root@localhost trigger xero_bill_ad
    after delete
    on xero_bill
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'xero_bill'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted xero_bill.id=', OLD.id)
    );
END;

