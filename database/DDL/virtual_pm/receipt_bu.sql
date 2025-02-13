create definer = root@localhost trigger receipt_bu
    before update
    on receipt
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

