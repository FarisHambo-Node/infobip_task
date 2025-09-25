-- Question 2: What is the share of segment A clients in the total number of clients?

SELECT 
    segment,
    COUNT(DISTINCT c.customer_id) as client_count,
    ROUND(
        COUNT(DISTINCT c.customer_id) * 100.0 / 
        SUM(COUNT(DISTINCT c.customer_id)) OVER (), 
        2
    ) as percentage_share
FROM customers c
GROUP BY segment
ORDER BY client_count DESC;
