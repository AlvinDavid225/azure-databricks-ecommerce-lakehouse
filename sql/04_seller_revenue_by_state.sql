-- Top 10 Sellers by State Revenue
-- Dashboard: Bar chart showing total revenue per seller state

SELECT
    s.seller_state,
    ROUND(SUM(f.price), 2) AS total_revenue
FROM ecommerce_dev.gold.fact_orders f
JOIN ecommerce_dev.gold.dim_seller s ON f.seller_id = s.seller_id
GROUP BY s.seller_state
ORDER BY total_revenue DESC
LIMIT 10
