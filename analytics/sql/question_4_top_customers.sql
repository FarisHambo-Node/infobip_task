-- Question 4: Who are the top 20 clients in terms of revenue generated?

SELECT 
    c.customer_id,
    SUM(t.revenue_eur) as total_revenue
FROM customers c
JOIN traffic t ON c.customer_id = t.customer_id
GROUP BY c.customer_id
ORDER BY total_revenue DESC
LIMIT 20;
