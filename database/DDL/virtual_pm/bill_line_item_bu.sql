create definer = root@localhost trigger bill_line_item_bu
    before update
    on xero_bill_line_item
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

