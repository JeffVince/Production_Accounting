create definer = root@localhost trigger xero_bill_bu
    before update
    on xero_bill
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

