-- Question 3: Make a list of clients who have changed segment within 12 months

SELECT 
    customer_id
FROM customers
WHERE customer_id IN (
    SELECT customer_id
    FROM (
        SELECT 
            c.customer_id,
            COUNT(DISTINCT c.segment) as segment_count
        FROM customers c
        INNER JOIN traffic t ON c.customer_id = t.customer_id
        WHERE t.send_date >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY c.customer_id
        HAVING COUNT(DISTINCT c.segment) >= 2
    ) segment_changes
);
