create definer = root@localhost trigger detail_item_ai
    after insert
    on detail_item
    for each row
BEGIN
    INSERT INTO audit_log (table_id, operation, record_id, message)
    VALUES (
        (SELECT id FROM sys_table WHERE name = 'detail_item'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted detail_item.id=', NEW.id)
    );
END;

