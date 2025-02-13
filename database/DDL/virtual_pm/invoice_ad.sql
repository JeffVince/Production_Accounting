create definer = root@localhost trigger invoice_ad
    after delete
    on invoice
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'invoice'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted invoice.id=', OLD.id)
    );
END;

