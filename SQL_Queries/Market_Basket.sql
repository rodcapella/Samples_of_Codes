# Análise de cesta de compras (Market Basket)
WITH product_pairs AS (
    SELECT
        a.product_id AS product_a,
        b.product_id AS product_b,
        COUNT(*) AS pair_count
    FROM order_items a
    JOIN order_items b
        ON a.order_id = b.order_id
        AND a.product_id < b.product_id
    GROUP BY 1,2
),
pair_frequency AS (
    SELECT
        product_a,
        product_b,
        pair_count,
        RANK() OVER (
            ORDER BY pair_count DESC
        ) AS rank
    FROM product_pairs
)

SELECT *
FROM pair_frequency
WHERE rank <= 10;
