create definer = root@localhost trigger bill_line_item_ad
    after delete
    on xero_bill_line_item
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'bill_line_item'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted bill_line_item.id=', OLD.id)
    );
END;

