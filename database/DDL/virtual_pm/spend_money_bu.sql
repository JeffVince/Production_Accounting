create definer = root@localhost trigger spend_money_bu
    before update
    on spend_money
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

