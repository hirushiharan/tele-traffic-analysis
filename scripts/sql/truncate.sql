-- Disable foreign key checks
SET FOREIGN_KEY_CHECKS = 0;

-- Truncate the tables
TRUNCATE TABLE calls;
TRUNCATE TABLE users;
TRUNCATE TABLE cell_towers;

-- Enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;
