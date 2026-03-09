# Detecta transações muito maiores que o comportamento normal do cliente.
WITH transaction_stats AS (
    SELECT
        user_id,
        transaction_time,
        amount,
        AVG(amount) OVER (
            PARTITION BY user_id
        ) AS avg_amount,
        STDDEV(amount) OVER (
            PARTITION BY user_id
        ) AS std_amount
    FROM transactions
)

SELECT
    user_id,
    transaction_time,
    amount,
    avg_amount,
    std_amount
FROM transaction_stats
WHERE amount > avg_amount + 3 * std_amount;
