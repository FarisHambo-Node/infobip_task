-- Analytics Schema for Business Questions Results
-- This schema stores the results of business analytics queries

-- Create business analytics schema
CREATE SCHEMA IF NOT EXISTS business_analytics;

-- Table for Question 1: Industry exposure analysis
CREATE TABLE IF NOT EXISTS business_analytics.industry_exposure (
    industry VARCHAR(100) PRIMARY KEY,
    total_revenue DECIMAL(15,2) NOT NULL,
    customer_count INTEGER NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Question 2: Segment analysis
CREATE TABLE IF NOT EXISTS business_analytics.segment_analysis (
    segment VARCHAR(100) PRIMARY KEY,
    client_count INTEGER NOT NULL,
    percentage_share DECIMAL(5,2) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Question 3: Recent customers (segment changes)
CREATE TABLE IF NOT EXISTS business_analytics.recent_customers (
    customer_id INTEGER PRIMARY KEY,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Question 4: Top 20 customers by revenue
CREATE TABLE IF NOT EXISTS business_analytics.top_customers (
    customer_id INTEGER PRIMARY KEY,
    total_revenue DECIMAL(15,2) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Question 5: Top 10% customers with 12-month revenue
CREATE TABLE IF NOT EXISTS business_analytics.monthly_active_customers (
    customer_id INTEGER PRIMARY KEY,
    total_revenue DECIMAL(15,2) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Indexes for better performance

CREATE INDEX IF NOT EXISTS idx_segment_analysis_client_count 
ON business_analytics.segment_analysis(client_count DESC);


CREATE INDEX IF NOT EXISTS idx_top_customers_revenue 
ON business_analytics.top_customers(total_revenue DESC);

CREATE INDEX IF NOT EXISTS idx_monthly_active_revenue 
ON business_analytics.monthly_active_customers(total_revenue DESC);

-- Function to clean old results (keep only last 30 days)
CREATE OR REPLACE FUNCTION business_analytics.cleanup_old_results()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM business_analytics.industry_exposure 
    WHERE last_updated < CURRENT_DATE - INTERVAL '30 days';
    
    DELETE FROM business_analytics.segment_analysis 
    WHERE last_updated < CURRENT_DATE - INTERVAL '30 days';
    
    DELETE FROM business_analytics.recent_customers 
    WHERE last_updated < CURRENT_DATE - INTERVAL '30 days';
    
    DELETE FROM business_analytics.top_customers 
    WHERE last_updated < CURRENT_DATE - INTERVAL '30 days';
    
    DELETE FROM business_analytics.monthly_active_customers 
    WHERE last_updated < CURRENT_DATE - INTERVAL '30 days';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
