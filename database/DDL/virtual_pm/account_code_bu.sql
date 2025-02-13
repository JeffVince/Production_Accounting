create definer = root@localhost trigger account_code_bu
    before update
    on account_code
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

