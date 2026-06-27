-- Customer Distribution by State
-- Dashboard: Bar chart showing unique customers per state

SELECT
    c.customer_state,
    COUNT(DISTINCT f.customer_id) AS total_customers
FROM ecommerce_dev.gold.fact_orders f
JOIN ecommerce_dev.gold.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY total_customers DESC
LIMIT 10
