# Streaming / Apps — Sessões de usuário
WITH event_gaps AS (
    SELECT
        user_id,
        event_time,
        LAG(event_time) OVER (
            PARTITION BY user_id
            ORDER BY event_time
        ) AS prev_event
    FROM app_events
),
session_flags AS (
    SELECT
        *,
        CASE
            WHEN prev_event IS NULL THEN 1
            WHEN event_time - prev_event > INTERVAL '30 minutes' THEN 1
            ELSE 0
        END AS new_session
    FROM event_gaps
),
sessions AS (
    SELECT
        *,
        SUM(new_session) OVER (
            PARTITION BY user_id
            ORDER BY event_time
        ) AS session_id
    FROM session_flags
)

SELECT *
FROM sessions;
