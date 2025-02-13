create definer = root@localhost trigger detail_item_au
    after update
    on detail_item
    for each row
BEGIN
    INSERT INTO audit_log (table_id, operation, record_id, message)
    VALUES (
        (SELECT id FROM sys_table WHERE name = 'detail_item'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated detail_item.id=', NEW.id)
    );
END;

