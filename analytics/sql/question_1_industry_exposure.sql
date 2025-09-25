-- Question 1: To which industry is the company most exposed to?

SELECT 
    c.industry,
    SUM(t.revenue_eur) as total_revenue,
    COUNT(DISTINCT c.customer_id) as customer_count
FROM customers c
LEFT JOIN traffic t ON c.customer_id = t.customer_id
GROUP BY c.industry
ORDER BY total_revenue DESC;
-- LIMIT 1;