create definer = root@localhost trigger tax_form_bu
    before update
    on tax_form
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

