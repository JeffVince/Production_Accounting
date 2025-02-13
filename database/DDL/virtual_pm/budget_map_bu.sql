create definer = root@localhost trigger budget_map_bu
    before update
    on budget_map
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

