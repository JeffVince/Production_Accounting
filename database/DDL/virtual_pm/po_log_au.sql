create definer = root@localhost trigger po_log_au
    after update
    on po_log
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'po_log'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated po_log.id=', NEW.id)
    );
END;

