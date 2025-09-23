-- Question 1: To which industry is the company most exposed to?
-- This query analyzes industry exposure by revenue and customer count

SELECT 
    c.industry,
    SUM(t.revenue_eur) as total_revenue,
    COUNT(DISTINCT c.customer_id) as customer_count,
    ROUND(AVG(t.revenue_eur), 2) as avg_revenue_per_transaction,
    ROUND(SUM(t.revenue_eur) * 100.0 / SUM(SUM(t.revenue_eur)) OVER (), 2) as revenue_share_percentage
FROM customers c
JOIN traffic t ON c.customer_id = t.customer_id
GROUP BY c.industry
ORDER BY total_revenue DESC;
