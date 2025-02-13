create definer = root@localhost trigger project_au
    after update
    on project
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'project'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated project.id=', NEW.id)
    );
END;

