create definer = root@localhost trigger contact_bu
    before update
    on contact
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

