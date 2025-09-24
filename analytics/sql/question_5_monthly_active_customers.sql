-- Question 5: Which top 10% of clients generate revenue over all 12 months?

WITH monthly_revenue AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.segment,
        c.industry,
        COUNT(DISTINCT DATE_TRUNC('month', t.send_date)) as months_with_revenue,
        SUM(t.revenue_eur) as total_revenue,
        COUNT(t.traffic_id) as total_transactions
    FROM customers c
    JOIN traffic t ON c.customer_id = t.customer_id
    WHERE t.send_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY c.customer_id, c.customer_name, c.segment, c.industry
    HAVING COUNT(DISTINCT DATE_TRUNC('month', t.send_date)) = 12
),
ranked_customers AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as revenue_rank,
           COUNT(*) OVER () as total_12_month_customers
    FROM monthly_revenue
)
SELECT 
    customer_id,
    customer_name,
    segment,
    industry,
    total_revenue,
    total_transactions,
    months_with_revenue,
    revenue_rank,
    total_12_month_customers,
    ROUND(revenue_rank * 100.0 / total_12_month_customers, 2) as percentile
FROM ranked_customers
WHERE revenue_rank <= CEIL(total_12_month_customers * 0.1)
ORDER BY total_revenue DESC;
