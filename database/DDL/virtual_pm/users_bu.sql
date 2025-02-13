create definer = root@localhost trigger users_bu
    before update
    on users
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

