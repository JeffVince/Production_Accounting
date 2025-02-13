create definer = root@localhost trigger contact_ai
    after insert
    on contact
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'contact'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted contact.id=', NEW.id)
    );
END;

