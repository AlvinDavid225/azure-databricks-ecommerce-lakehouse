-- Top 10 Product Categories by Revenue
-- Dashboard: Donut chart showing category revenue distribution

SELECT
    p.product_category_name,
    ROUND(SUM(f.price), 2) AS total_revenue
FROM ecommerce_dev.gold.fact_orders f
JOIN ecommerce_dev.gold.dim_product p ON f.product_id = p.product_id
WHERE p.product_category_name IS NOT NULL
GROUP BY p.product_category_name
ORDER BY total_revenue DESC
LIMIT 10
