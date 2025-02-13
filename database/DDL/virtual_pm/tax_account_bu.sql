create definer = root@localhost trigger tax_account_bu
    before update
    on tax_account
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

