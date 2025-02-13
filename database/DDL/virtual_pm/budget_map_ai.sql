create definer = root@localhost trigger budget_map_ai
    after insert
    on budget_map
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'budget_map'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted budget_map.id=', NEW.id)
    );
END;

