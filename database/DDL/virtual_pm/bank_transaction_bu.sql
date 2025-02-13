create definer = root@localhost trigger bank_transaction_bu
    before update
    on bank_transaction
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

