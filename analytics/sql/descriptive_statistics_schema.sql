-- Descriptive Statistics Schema
-- This schema stores descriptive statistics for all numeric columns across all tables

-- Create descriptive statistics schema
CREATE SCHEMA IF NOT EXISTS descriptive_statistics;

-- Table for storing descriptive statistics summary
CREATE TABLE IF NOT EXISTS descriptive_statistics.column_statistics (
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

-- Index for better performance
CREATE INDEX IF NOT EXISTS idx_column_statistics_table 
ON descriptive_statistics.column_statistics(table_name);

CREATE INDEX IF NOT EXISTS idx_column_statistics_updated 
ON descriptive_statistics.column_statistics(last_updated DESC);

