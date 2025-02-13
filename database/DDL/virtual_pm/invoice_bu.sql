create definer = root@localhost trigger invoice_bu
    before update
    on invoice
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

