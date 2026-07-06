-- Total records
SELECT COUNT(*) AS total_records
FROM saas_events;

-- Check for NULL values in mandatory fields
SELECT *
FROM saas_events
WHERE event_id IS NULL
   OR user_id IS NULL
   OR organization_id IS NULL
   OR event_type IS NULL
   OR country IS NULL
   OR subscription_plan IS NULL
   OR timestamp IS NULL;

-- Check for duplicate event IDs
SELECT
    event_id,
    COUNT(*) AS duplicate_count
FROM saas_events
GROUP BY event_id
HAVING COUNT(*) > 1;