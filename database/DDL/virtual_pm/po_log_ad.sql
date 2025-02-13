create definer = root@localhost trigger po_log_ad
    after delete
    on po_log
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'po_log'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted po_log.id=', OLD.id)
    );
END;

