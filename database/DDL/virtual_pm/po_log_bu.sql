create definer = root@localhost trigger po_log_bu
    before update
    on po_log
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

