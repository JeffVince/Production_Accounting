create definer = root@localhost trigger tax_form_au
    after update
    on tax_form
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_form'),
        'UPDATE',
        NEW.id,
        CONCAT('Updated tax_form.id=', NEW.id)
    );
END;

