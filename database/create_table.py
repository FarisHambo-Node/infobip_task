#!/usr/bin/env python3
"""
Script to create the customers_revenue_by_period table in RDS PostgreSQL database
"""

import psycopg2
import sys
from psycopg2 import sql

# Database connection parameters from Terraform configuration
DB_CONFIG = {
    "host": "data-engineering-task-dev-postgres.c34qoko2amod.eu-north-1.rds.amazonaws.com",
    "database": "infobip_task",
    "user": "postgres",
    "password": "Infobip2025!",
    "port": 5432,
}


def create_table():
    """Create the customers_revenue_by_period table"""

    # SQL to create the table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS customers_revenue_by_period (
        customer_id INTEGER PRIMARY KEY,
        revenue_last_month DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        revenue_last_quarter DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        revenue_mtd DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        revenue_ytd DECIMAL(12,2) NOT NULL DEFAULT 0.00,
        revenue_increase_pct_qoq DECIMAL(5,2) DEFAULT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    try:
        print("Connecting to RDS PostgreSQL database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("Creating customers_revenue_by_period table...")
        cursor.execute(create_table_sql)

        # Commit the transaction
        conn.commit()
        print("Table created successfully!")

        # Verify table exists
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'customers_revenue_by_period'
            );
        """
        )

        table_exists = cursor.fetchone()[0]
        if table_exists:
            print("Table verification: customers_revenue_by_period exists")
        else:
            print(
                "Table verification failed: customers_revenue_by_period does not exist"
            )

        # Show table structure
        cursor.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'customers_revenue_by_period'
            ORDER BY ordinal_position;
        """
        )

        columns = cursor.fetchall()
        print("\nTable structure:")
        print("Column Name | Data Type | Nullable | Default")
        print("-" * 50)
        for col in columns:
            print(f"{col[0]:<15} | {col[1]:<15} | {col[2]:<8} | {col[3] or 'None'}")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        if "cursor" in locals():
            cursor.close()
        if "conn" in locals():
            conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    create_table()
