-- Question 4: Who are the top 20 clients in terms of revenue generated?

SELECT 
    c.customer_id,
    c.customer_name,
    c.segment,
    c.industry,
    c.country,
    SUM(t.revenue_eur) as total_revenue,
    COUNT(t.traffic_id) as total_transactions,
    ROUND(AVG(t.revenue_eur), 2) as avg_revenue_per_transaction,
    MIN(t.send_date) as first_transaction_date,
    MAX(t.send_date) as last_transaction_date
FROM customers c
JOIN traffic t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.customer_name, c.segment, c.industry, c.country
ORDER BY total_revenue DESC
LIMIT 20;
