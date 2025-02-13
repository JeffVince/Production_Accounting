create definer = root@localhost trigger contact_ad
    after delete
    on contact
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'contact'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted contact.id=', OLD.id)
    );
END;

