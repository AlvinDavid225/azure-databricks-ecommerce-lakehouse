-- Order Status Distribution
-- Dashboard: Bar chart showing count of orders per status

SELECT
    order_status,
    COUNT(*) AS total_orders
FROM ecommerce_dev.gold.fact_orders
GROUP BY order_status
ORDER BY total_orders DESC
