create definer = root@localhost trigger receipt_ad
    after delete
    on receipt
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'receipt'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted receipt.id=', OLD.id)
    );
END;

