create definer = root@localhost trigger purchase_order_bu
    before update
    on purchase_order
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

