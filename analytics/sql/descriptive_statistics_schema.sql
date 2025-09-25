-- Descriptive Statistics Schema
-- This schema stores descriptive statistics for industry_exposure table and extended categories

-- Create descriptive statistics schema
CREATE SCHEMA IF NOT EXISTS descriptive_statistics;

-- Table for storing descriptive statistics summary
DROP TABLE IF EXISTS descriptive_statistics.column_statistics CASCADE;
CREATE TABLE descriptive_statistics.column_statistics (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    min_value DECIMAL(20,4),
    max_value DECIMAL(20,4),
    mean_value DECIMAL(20,4),
    median_value DECIMAL(20,4),
    std_deviation DECIMAL(20,4),
    count_non_null INTEGER,
    count_total INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT uk_table_column UNIQUE (table_name, column_name)
);

-- Extended industry exposure table with categories
DROP TABLE IF EXISTS descriptive_statistics.industry_exposure_extended CASCADE;
CREATE TABLE descriptive_statistics.industry_exposure_extended (
    industry VARCHAR(100) PRIMARY KEY,
    total_revenue DECIMAL(15,2) NOT NULL,
    customer_count INTEGER NOT NULL,
    total_revenue_category VARCHAR(10) NOT NULL,
    customer_count_category VARCHAR(10) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);