create definer = root@localhost trigger po_log_ai
    after insert
    on po_log
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'po_log'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted po_log.id=', NEW.id)
    );
END;

