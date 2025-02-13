create definer = root@localhost trigger project_ad
    after delete
    on project
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'project'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted project.id=', OLD.id)
    );
END;

