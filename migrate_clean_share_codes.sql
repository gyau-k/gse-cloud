-- =============================================================================
-- GSE: One-time share_code cleanup migration
-- =============================================================================
-- What this does:
--   1. Strips asterisks from share_code          (**MTNGH** → MTNGH)
--   2. Uppercases share_code                     (mtngh     → MTNGH)
--   3. Collapses spaces                          (SCB PREF  → SCBPREF)
--   4. Where cleaning would create a duplicate (date, share_code), the row
--      with the lower total_value_traded is deleted first so the UPDATE
--      never hits the unique constraint.
--
-- Safe to run multiple times — subsequent runs find nothing to change.
-- Run this BEFORE deploying the updated Python scripts.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- Step 1: Show what will be affected (informational — no changes yet)
-- -----------------------------------------------------------------------------
SELECT
    share_code                                              AS raw_code,
    REPLACE(UPPER(REPLACE(share_code, '*', '')), ' ', '')  AS clean_code,
    COUNT(*)                                                AS rows
FROM daily_trading
WHERE share_code IS DISTINCT FROM
      REPLACE(UPPER(REPLACE(share_code, '*', '')), ' ', '')
GROUP BY raw_code, clean_code
ORDER BY raw_code;

-- -----------------------------------------------------------------------------
-- Step 2: Where cleaning would produce a duplicate (date, clean_code),
--         delete the row with the LOWER total_value_traded.
--         (If both are NULL or equal, delete the one with the higher id.)
-- -----------------------------------------------------------------------------
DELETE FROM daily_trading
WHERE id IN (
    SELECT loser_id
    FROM (
        SELECT
            d.id                                                   AS dirty_id,
            d.share_code                                           AS dirty_code,
            REPLACE(UPPER(REPLACE(d.share_code, '*', '')), ' ', '') AS clean_code,
            d.date,
            d.total_value_traded                                   AS dirty_val,
            -- Find if a clean version already exists for the same date
            c.id                                                   AS clean_id,
            c.total_value_traded                                   AS clean_val,
            -- Decide which row loses: lower value traded (or higher id if tied)
            CASE
                WHEN COALESCE(d.total_value_traded, 0) >= COALESCE(c.total_value_traded, 0)
                THEN c.id
                ELSE d.id
            END AS loser_id
        FROM daily_trading d
        JOIN daily_trading c
          ON c.date = d.date
         AND c.share_code = REPLACE(UPPER(REPLACE(d.share_code, '*', '')), ' ', '')
         AND c.id <> d.id
        WHERE d.share_code IS DISTINCT FROM
              REPLACE(UPPER(REPLACE(d.share_code, '*', '')), ' ', '')
    ) conflicts
    WHERE loser_id IS NOT NULL
);

-- -----------------------------------------------------------------------------
-- Step 3: Now safe to rename — no duplicates remain
-- -----------------------------------------------------------------------------
UPDATE daily_trading
SET share_code = REPLACE(UPPER(REPLACE(share_code, '*', '')), ' ', '')
WHERE share_code IS DISTINCT FROM
      REPLACE(UPPER(REPLACE(share_code, '*', '')), ' ', '');

-- -----------------------------------------------------------------------------
-- Step 4: Verify — should return 0 rows if all clean
-- -----------------------------------------------------------------------------
SELECT
    share_code,
    COUNT(*) AS rows
FROM daily_trading
WHERE share_code ~ '\*'                          -- still has asterisks
   OR share_code != UPPER(share_code)            -- still lowercase
   OR share_code LIKE '% %'                      -- still has spaces
GROUP BY share_code
ORDER BY share_code;

-- -----------------------------------------------------------------------------
-- Step 5: Final count per stock (sanity check)
-- -----------------------------------------------------------------------------
SELECT share_code, COUNT(*) AS trading_days
FROM daily_trading
GROUP BY share_code
ORDER BY share_code;

COMMIT;
