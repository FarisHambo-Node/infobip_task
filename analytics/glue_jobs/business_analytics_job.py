import sys
import boto3
import psycopg2
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Stupid comment to trigger a build

# Glue job parameters
logger.info("Getting job parameters...")
try:
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "S3_BUCKET",
            "ARTIFACTS_BUCKET",
            "DB_HOST",
            "DB_NAME",
            "DB_USER",
            "DB_PASSWORD",
        ],
    )
    logger.info(f"Job parameters retrieved successfully: {list(args.keys())}")
except Exception as e:
    logger.error(f"Failed to get job parameters: {str(e)}")
    raise

# Initialize Glue context
logger.info("Initializing Glue context...")
try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    logger.info("Glue context initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Glue context: {str(e)}")
    raise


def get_db_connection():
    """Get RDS PostgreSQL connection"""
    logger.info("Connecting to RDS database...")
    try:
        conn = psycopg2.connect(
            host=args["DB_HOST"],
            database=args["DB_NAME"],
            user=args["DB_USER"],
            password=args["DB_PASSWORD"],
        )
        logger.info("Successfully connected to RDS database")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to RDS database: {str(e)}")
        raise


def load_data_from_s3_to_rds():
    """Load CSV data from S3 to RDS tables"""
    logger.info("Starting data load from S3 to RDS...")
    print("Starting data load from S3 to RDS...")

    # S3 paths
    s3_bucket = args["S3_BUCKET"]
    customers_path = f"s3://{s3_bucket}/raw_data/customers.csv"
    traffic_path = f"s3://{s3_bucket}/raw_data/traffic.csv"
    channels_path = f"s3://{s3_bucket}/raw_data/channels.csv"

    logger.info(f"Reading data from S3 bucket: {s3_bucket}")

    # Read CSV files
    logger.info("Reading CSV files from S3...")
    try:
        customers_df = spark.read.option("header", "true").csv(customers_path)
        traffic_df = spark.read.option("header", "true").csv(traffic_path)
        channels_df = spark.read.option("header", "true").csv(channels_path)
        logger.info("Successfully read CSV files from S3")
    except Exception as e:
        logger.error(f"Failed to read CSV files from S3: {str(e)}")
        raise

    # Convert to pandas for easier database operations
    logger.info("Converting Spark DataFrames to Pandas...")
    try:
        customers_pandas = customers_df.toPandas()
        traffic_pandas = traffic_df.toPandas()
        channels_pandas = channels_df.toPandas()
        logger.info(
            f"Converted to Pandas - Customers: {len(customers_pandas)}, Traffic: {len(traffic_pandas)}, Channels: {len(channels_pandas)}"
        )
    except Exception as e:
        logger.error(f"Failed to convert to Pandas: {str(e)}")
        raise

    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Setup base schema first
        artifacts_bucket = args["ARTIFACTS_BUCKET"]
        setup_base_schema(cursor, artifacts_bucket)

        # Upsert customers data
        logger.info("Upserting customers data...")
        for _, row in customers_pandas.iterrows():
            cursor.execute(
                """
                INSERT INTO customers (account_id, customer_name, segment, industry, country, created_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (account_id) 
                DO UPDATE SET 
                    customer_name = EXCLUDED.customer_name,
                    segment = EXCLUDED.segment,
                    industry = EXCLUDED.industry,
                    country = EXCLUDED.country,
                    created_date = EXCLUDED.created_date
            """,
                (
                    row["account_id"],
                    row["customer_name"],
                    row["segment"],
                    row["industry"],
                    row["country"],
                    row["created_date"],
                ),
            )

        # Upsert channels data
        logger.info("Upserting channels data...")
        for _, row in channels_pandas.iterrows():
            cursor.execute(
                """
                INSERT INTO channels (channel_name, channel_type, unit_price_eur)
                VALUES (%s, %s, %s)
                ON CONFLICT (channel_name) 
                DO UPDATE SET 
                    channel_type = EXCLUDED.channel_type,
                    unit_price_eur = EXCLUDED.unit_price_eur
            """,
                (row["channel_name"], row["channel_type"], row["unit_price_eur"]),
            )

        # Upsert traffic data
        logger.info("Upserting traffic data...")
        for _, row in traffic_pandas.iterrows():
            # Get customer_id from customers table based on account_id
            cursor.execute(
                "SELECT customer_id FROM customers WHERE account_id = %s",
                (row["account_id"],),
            )
            customer_result = cursor.fetchone()
            if customer_result:
                customer_id = customer_result[0]
            else:
                logger.warning(
                    f"Customer with account_id {row['account_id']} not found, skipping traffic record"
                )
                continue

            # Get channel_id from channels table based on channel_id from CSV
            cursor.execute(
                "SELECT channel_id FROM channels WHERE channel_id = %s",
                (row["channel_id"],),
            )
            channel_result = cursor.fetchone()
            if channel_result:
                channel_id = channel_result[0]
            else:
                logger.warning(
                    f"Channel with channel_id {row['channel_id']} not found, skipping traffic record"
                )
                continue

            cursor.execute(
                """
                INSERT INTO traffic (send_date, account_id, customer_id, channel_id, 
                                   interactions_count, revenue_eur)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (send_date, account_id, customer_id, channel_id) 
                DO UPDATE SET 
                    interactions_count = EXCLUDED.interactions_count,
                    revenue_eur = EXCLUDED.revenue_eur
            """,
                (
                    row["send_date"],
                    row["account_id"],
                    customer_id,
                    channel_id,
                    row["interactions_count"],
                    row["revenue_eur"],
                ),
            )

        conn.commit()
        logger.info("Data successfully loaded to RDS")
        print("Data successfully loaded to RDS")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading data: {str(e)}")
        print(f"Error loading data: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
        logger.info("Database connection closed")


def execute_sql_file(cursor, s3_bucket, sql_file_path, return_results=True):
    """Execute SQL file from S3"""
    logger.info(f"Executing SQL file: {sql_file_path}")

    # Read SQL file from S3
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=sql_file_path)
        sql_content = response["Body"].read().decode("utf-8")
        logger.info(f"Successfully read SQL file: {sql_file_path}")
    except Exception as e:
        logger.error(f"Failed to read SQL file {sql_file_path}: {str(e)}")
        raise

    # Execute SQL
    try:
        cursor.execute(sql_content)
        if return_results:
            results = cursor.fetchall()
            logger.info(f"Successfully executed SQL file: {sql_file_path}")
            return results
        else:
            logger.info(f"Successfully executed SQL file: {sql_file_path}")
            return None
    except Exception as e:
        logger.error(f"Failed to execute SQL file {sql_file_path}: {str(e)}")
        raise


def setup_base_schema(cursor, artifacts_bucket):
    """Setup base database schema (customers, traffic, channels tables)"""
    logger.info("Setting up base database schema...")

    # Execute base schema SQL (no results expected)
    execute_sql_file(
        cursor, artifacts_bucket, "database/schema.sql", return_results=False
    )
    logger.info("Base database schema setup completed")


def setup_analytics_schema(cursor, artifacts_bucket):
    """Setup analytics schema and tables"""
    logger.info("Setting up analytics schema...")

    # Execute analytics schema SQL (no results expected)
    execute_sql_file(
        cursor, artifacts_bucket, "sql/analytics_schema.sql", return_results=False
    )
    logger.info("Analytics schema setup completed")


def execute_business_queries():
    """Execute the 5 business questions and store results"""
    logger.info("Executing business analytics queries...")
    print("Executing business analytics queries...")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Setup analytics schema first
        artifacts_bucket = args["ARTIFACTS_BUCKET"]
        setup_analytics_schema(cursor, artifacts_bucket)

        # Question 1: To which industry is the company most exposed to?
        logger.info("Executing Question 1: Industry exposure analysis...")
        results1 = execute_sql_file(
            cursor, artifacts_bucket, "sql/question_1_industry_exposure.sql"
        )

        # Clear and insert industry exposure data
        cursor.execute("TRUNCATE TABLE business_analytics.industry_exposure;")
        for row in results1:
            cursor.execute(
                """
                INSERT INTO business_analytics.industry_exposure 
                (industry, total_revenue, customer_count, avg_revenue_per_transaction, revenue_share_percentage)
                VALUES (%s, %s, %s, %s, %s);
            """,
                (row[0], float(row[1]), row[2], float(row[3]), float(row[4])),
            )

        logger.info(f"Inserted {len(results1)} industry records")

        # Question 2: What is the share of segment A clients in the total number of clients?
        logger.info("Executing Question 2: Segment analysis...")
        results2 = execute_sql_file(
            cursor, artifacts_bucket, "sql/question_2_segment_analysis.sql"
        )

        # Clear and insert segment analysis data
        cursor.execute("TRUNCATE TABLE business_analytics.segment_analysis;")
        for row in results2:
            cursor.execute(
                """
                INSERT INTO business_analytics.segment_analysis 
                (segment, client_count, percentage_share, total_revenue, avg_revenue_per_customer)
                VALUES (%s, %s, %s, %s, %s);
            """,
                (
                    row[0],
                    row[1],
                    float(row[2]),
                    float(row[3]) if row[3] else 0,
                    float(row[4]) if row[4] else 0,
                ),
            )

        logger.info(f"Inserted {len(results2)} segment records")

        # Question 3: Make a list of clients who have changed segment within 12 months
        logger.info("Executing Question 3: Recent customers analysis...")
        results3 = execute_sql_file(
            cursor, artifacts_bucket, "sql/question_3_recent_customers.sql"
        )

        # Clear and insert recent customers data
        cursor.execute("TRUNCATE TABLE business_analytics.recent_customers;")
        for row in results3:
            cursor.execute(
                """
                INSERT INTO business_analytics.recent_customers 
                (customer_id, customer_name, segment, industry, created_date, status)
                VALUES (%s, %s, %s, %s, %s, %s);
            """,
                (row[0], row[1], row[2], row[3], row[4], row[5]),
            )

        logger.info(f"Inserted {len(results3)} recent customer records")

        # Question 4: Who are the top 20 clients in terms of revenue generated?
        logger.info("Executing Question 4: Top 20 customers by revenue...")
        results4 = execute_sql_file(
            cursor, artifacts_bucket, "sql/question_4_top_customers.sql"
        )

        # Clear and insert top customers data
        cursor.execute("TRUNCATE TABLE business_analytics.top_customers;")
        for i, row in enumerate(results4, 1):
            cursor.execute(
                """
                INSERT INTO business_analytics.top_customers 
                (rank_position, customer_id, customer_name, segment, industry, country, 
                 total_revenue, total_transactions, avg_revenue_per_transaction, 
                 first_transaction_date, last_transaction_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
                (
                    i,
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    float(row[5]),
                    row[6],
                    float(row[7]),
                    row[8],
                    row[9],
                ),
            )

        logger.info(f"Inserted {len(results4)} top customer records")

        # Question 5: Which top 10% of clients generate revenue over all 12 months?
        logger.info("Executing Question 5: Top 10% customers with 12-month revenue...")
        results5 = execute_sql_file(
            cursor, artifacts_bucket, "sql/question_5_monthly_active_customers.sql"
        )

        # Clear and insert monthly active customers data
        cursor.execute("TRUNCATE TABLE business_analytics.monthly_active_customers;")
        for row in results5:
            cursor.execute(
                """
                INSERT INTO business_analytics.monthly_active_customers 
                (customer_id, customer_name, segment, industry, total_revenue, 
                 total_transactions, months_with_revenue, revenue_rank, percentile)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
                (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    float(row[4]),
                    row[5],
                    row[6],
                    row[7],
                    float(row[9]),
                ),
            )

        logger.info(f"Inserted {len(results5)} monthly active customer records")

        conn.commit()
        logger.info("Business analytics queries executed successfully")
        print("Business analytics queries executed successfully")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error executing business queries: {str(e)}")
        print(f"Error executing business queries: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
        logger.info("Database connection closed")


def main():
    """Main function to run the analytics job"""
    logger.info(f"Starting business analytics job at {datetime.now()}")
    print(f"Starting business analytics job at {datetime.now()}")

    try:
        # Step 1: Load data from S3 to RDS
        load_data_from_s3_to_rds()

        # Step 2: Execute business queries
        execute_business_queries()

        logger.info(
            f"Business analytics job completed successfully at {datetime.now()}"
        )
        print(f"Business analytics job completed successfully at {datetime.now()}")

    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        print(f"Job failed with error: {str(e)}")
        raise


if __name__ == "__main__":
    logger.info("Starting Glue job execution...")
    main()
    logger.info("Committing Glue job...")
    job.commit()
    logger.info("Glue job committed successfully")
    logger.info("Glue job execution completed successfully")
