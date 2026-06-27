-- Revenue Trend by Month
-- Dashboard: Line chart showing monthly revenue from Oct 2016 to Jul 2018

SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    ROUND(SUM(price), 2) AS total_revenue
FROM ecommerce_dev.gold.fact_orders
GROUP BY order_month
ORDER BY order_month ASC
