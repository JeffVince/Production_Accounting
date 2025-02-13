create definer = root@localhost trigger tax_form_ai
    after insert
    on tax_form
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_form'),
        'INSERT',
        NEW.id,
        CONCAT('Inserted tax_form.id=', NEW.id)
    );
END;

