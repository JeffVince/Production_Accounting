-- Step 1: Create a temporary table listing the "keeper" rows
DROP TABLE IF EXISTS keeper_rows;
CREATE TEMPORARY TABLE keeper_rows AS
SELECT
    name,
    MIN(id) AS keep_id
FROM contact
WHERE pulse_id IS NOT NULL
GROUP BY name;

-- Step 2: Delete rows that are NOT in the set of keeper rows.
--         If there is no corresponding keeper row for a given `name`,
--         it removes all rows of that name.
DELETE c
FROM contact c
LEFT JOIN keeper_rows k
    ON c.name = k.name
    AND c.id = k.keep_id
WHERE k.keep_id IS NULL
    OR c.id != k.keep_id;

-- (Optional) Drop the temporary table when done
DROP TABLE keeper_rows;