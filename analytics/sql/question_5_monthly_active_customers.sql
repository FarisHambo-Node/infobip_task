-- Question 5: Which top 10% of clients generate revenue over all 12 months?

-- CTE 1: Top 10% of clients by total revenue
WITH top_10_percent AS (
    SELECT 
        c.customer_id,
        SUM(t.revenue_eur) as total_revenue
    FROM customers c
    JOIN traffic t ON c.customer_id = t.customer_id
    GROUP BY c.customer_id
    ORDER BY total_revenue DESC
    LIMIT (SELECT CEIL(COUNT(DISTINCT c.customer_id) * 0.1) FROM customers c)
),
-- CTE 2: Last 12 months data
last_12_months AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', send_date) as month
    FROM traffic
    WHERE send_date >= CURRENT_DATE - INTERVAL '12 months'
),
-- CTE 3: Check which customers have activity in all 12 months
monthly_active AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT month) as months_with_activity
    FROM last_12_months
    GROUP BY customer_id
    HAVING COUNT(DISTINCT month) = 12
)
-- Final result: Top 10% customers who are active in all 12 months
SELECT 
    t.customer_id,
    t.total_revenue
FROM top_10_percent t
INNER JOIN monthly_active m ON t.customer_id = m.customer_id
ORDER BY t.total_revenue DESC;
