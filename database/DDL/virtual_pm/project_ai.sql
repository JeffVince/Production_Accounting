create definer = root@localhost trigger project_ai
    after insert
    on project
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'project'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted project.id=', NEW.id)
    );
END;

