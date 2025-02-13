create definer = root@localhost trigger contact_au
    after update
    on contact
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'contact'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated contact.id=', NEW.id)
    );
END;

