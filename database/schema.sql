DROP TABLE IF EXISTS traffic CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS channels CASCADE;

-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL UNIQUE,
    customer_name VARCHAR(255) NOT NULL,
    segment VARCHAR(100) NOT NULL,
    industry VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    created_date DATE NOT NULL DEFAULT CURRENT_DATE
);

-- Channels table
CREATE TABLE IF NOT EXISTS channels (
    channel_id SERIAL PRIMARY KEY,
    channel_name VARCHAR(100) NOT NULL UNIQUE,
    channel_type VARCHAR(50) NOT NULL,
    unit_price_eur DECIMAL(10,4) NOT NULL
);

-- Traffic table
CREATE TABLE IF NOT EXISTS traffic (
    traffic_id SERIAL PRIMARY KEY,
    send_date DATE NOT NULL,
    account_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    interactions_count INTEGER NOT NULL DEFAULT 0,
    revenue_eur DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    
    -- Foreign key constraints
    CONSTRAINT fk_traffic_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT fk_traffic_channel FOREIGN KEY (channel_id) REFERENCES channels(channel_id),
    
    -- Check constraints
    CONSTRAINT chk_interactions_positive CHECK (interactions_count >= 0),
    CONSTRAINT chk_revenue_positive CHECK (revenue_eur >= 0),
    
    -- Unique constraint for upsert
    CONSTRAINT uk_traffic_unique UNIQUE (send_date, account_id, customer_id, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_traffic_send_date ON traffic(send_date);
CREATE INDEX IF NOT EXISTS idx_traffic_account_id ON traffic(account_id);
CREATE INDEX IF NOT EXISTS idx_customers_account_id ON customers(account_id);

-- Customers revenue by period table
CREATE TABLE IF NOT EXISTS customers_revenue_by_period (
    customer_id INTEGER PRIMARY KEY,
    revenue_last_month DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    revenue_last_quarter DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    revenue_mtd DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    revenue_ytd DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    revenue_increase_pct_qoq DECIMAL(5,2) DEFAULT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
