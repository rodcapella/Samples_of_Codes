# Usuários que tiveram login por pelo menos 5 dias consecutivos.
WITH ordered_logins AS (
    SELECT
        user_id,
        login_date,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY login_date
        ) AS rn
    FROM logins
),
login_groups AS (
    SELECT
        user_id,
        login_date,
        login_date - rn * INTERVAL '1 day' AS grp
    FROM ordered_logins
),
streaks AS (
    SELECT
        user_id,
        MIN(login_date) AS start_streak,
        MAX(login_date) AS end_streak,
        COUNT(*) AS streak_length
    FROM login_groups
    GROUP BY user_id, grp
)

SELECT *
FROM streaks
WHERE streak_length >= 5;
