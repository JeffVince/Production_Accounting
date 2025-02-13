create definer = root@localhost trigger detail_item_bu
    before update
    on detail_item
    for each row
BEGIN
    SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

