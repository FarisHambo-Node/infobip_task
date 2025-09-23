-- Question 2: What is the share of segment A clients in the total number of clients?
-- This query analyzes customer segments and their distribution

SELECT 
    segment,
    COUNT(*) as client_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage_share,
    SUM(t.revenue_eur) as total_revenue,
    ROUND(AVG(t.revenue_eur), 2) as avg_revenue_per_customer
FROM customers c
LEFT JOIN traffic t ON c.customer_id = t.customer_id
GROUP BY segment
ORDER BY client_count DESC;
