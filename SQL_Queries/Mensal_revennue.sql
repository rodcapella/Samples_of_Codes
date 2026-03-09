# Receita mensal com crescimento mês a mês
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        SUM(amount) AS revenue
    FROM orders
    GROUP BY DATE_TRUNC('month', order_date)
),
revenue_growth AS (
    SELECT
        month,
        revenue,
        LAG(revenue) OVER (ORDER BY month) AS previous_month
    FROM monthly_revenue
)

SELECT
    month,
    revenue,
    previous_month,
    ROUND(
        (revenue - previous_month) / previous_month * 100, 
        2
    ) AS growth_percent
FROM revenue_growth;
