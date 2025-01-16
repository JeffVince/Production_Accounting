-- First, update existing rows to set map_code = 'Ophelia 2024'
UPDATE account_code
SET map_code = 'OPH_2024'
WHERE map_code = 'Ophelia 2024';
   -- Adjust WHERE conditions as needed (e.g., if you want to selectively update)


-- First, update existing rows to set map_code = 'Ophelia 2024'
UPDATE account_code
SET map_code = 'OPH_2025'
WHERE map_code = 'Ophelia 2025';
   -- Adjust WHERE conditions as needed (e.g., if you want to selectively update)




-- Now, insert duplicates with map_code = 'Ophelia 2025'
INSERT INTO account_code (
    code,
    map_code,
    tax_id,
    account_description
    -- You can list other columns if you want to copy them (except the primary key,
    -- which should auto-increment).
)
SELECT
    code,
    'OPH_2025',
    tax_id,
    account_description
    -- Include the same columns as above for duplication.
FROM account_code
WHERE map_code = 'Ophelia 2025';