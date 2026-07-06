-- Events by event type
SELECT
    event_type,
    COUNT(*) AS total_events
FROM saas_events
GROUP BY event_type
ORDER BY total_events DESC;

-- Events by subscription plan
SELECT
    subscription_plan,
    COUNT(*) AS total_events
FROM saas_events
GROUP BY subscription_plan
ORDER BY total_events DESC;

-- Events by country
SELECT
    country,
    COUNT(*) AS total_events
FROM saas_events
GROUP BY country
ORDER BY total_events DESC;

-- Daily event volume
SELECT
    DATE(timestamp) AS event_date,
    COUNT(*) AS total_events
FROM saas_events
GROUP BY DATE(timestamp)
ORDER BY event_date;

-- Top 10 most active users
SELECT
    user_id,
    COUNT(*) AS total_events
FROM saas_events
GROUP BY user_id
ORDER BY total_events DESC
LIMIT 10;