-- Analytics Schema for Business Questions Results
-- This schema stores the results of business analytics queries

-- Create business analytics schema
CREATE SCHEMA IF NOT EXISTS business_analytics;

-- Table for Question 1: Industry exposure analysis
DROP TABLE IF EXISTS business_analytics.industry_exposure CASCADE;
CREATE TABLE business_analytics.industry_exposure (
    industry VARCHAR(100) PRIMARY KEY,
    total_revenue DECIMAL(15,2) NOT NULL,
    customer_count INTEGER NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Question 2: Segment analysis
DROP TABLE IF EXISTS business_analytics.segment_analysis CASCADE;
CREATE TABLE business_analytics.segment_analysis (
    segment VARCHAR(100) PRIMARY KEY,
    client_count INTEGER NOT NULL,
    percentage_share DECIMAL(5,2) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Question 3: Recent customers (segment changes)
DROP TABLE IF EXISTS business_analytics.recent_customers CASCADE;
CREATE TABLE business_analytics.recent_customers (
    customer_id INTEGER PRIMARY KEY,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Question 4: Top 20 customers by revenue
DROP TABLE IF EXISTS business_analytics.top_customers CASCADE;
CREATE TABLE business_analytics.top_customers (
    customer_id INTEGER PRIMARY KEY,
    total_revenue DECIMAL(15,2) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Question 5: Top 10% customers with 12-month revenue
DROP TABLE IF EXISTS business_analytics.monthly_active_customers CASCADE;
CREATE TABLE business_analytics.monthly_active_customers (
    customer_id INTEGER PRIMARY KEY,
    total_revenue DECIMAL(15,2) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
