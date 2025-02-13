create definer = root@localhost trigger tax_form_ad
    after delete
    on tax_form
    for each row
BEGIN
    INSERT INTO `audit_log` (table_id, operation, record_id, message)
    VALUES (
        (SELECT `id` FROM `sys_table` WHERE `name` = 'tax_form'),
        'DELETE',
        OLD.id,
        CONCAT('Deleted tax_form.id=', OLD.id)
    );
END;

