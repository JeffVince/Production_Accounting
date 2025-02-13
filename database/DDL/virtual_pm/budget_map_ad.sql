create definer = root@localhost trigger budget_map_ad
    after delete
    on budget_map
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'budget_map'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted budget_map.id=', OLD.id)
    );
END;

