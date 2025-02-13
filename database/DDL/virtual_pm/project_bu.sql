create definer = root@localhost trigger project_bu
    before update
    on project
    for each row
BEGIN
  SET NEW.updated_at = CURRENT_TIMESTAMP;
END;

