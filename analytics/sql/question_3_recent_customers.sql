-- Question 3: Make a list of clients who have changed segment within 12 months
-- Note: This is simplified since we don't have historical segment data
-- In a real scenario, you would need a customer_segment_history table

SELECT 
    c.customer_id,
    c.customer_name,
    c.segment,
    c.industry,
    c.created_date,
    CASE 
        WHEN c.created_date >= CURRENT_DATE - INTERVAL '12 months' 
        THEN 'Recently Added/Changed'
        ELSE 'Long-term Customer'
    END as status
FROM customers c
WHERE c.created_date >= CURRENT_DATE - INTERVAL '12 months'
ORDER BY c.created_date DESC;
