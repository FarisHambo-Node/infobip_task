import sys
import boto3
import psycopg2
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
from datetime import datetime
import logging
from pyspark.sql.functions import (
    col,
    min as spark_min,
    max as spark_max,
    mean,
    stddev,
    count,
    expr,
    when,
    percentile_approx,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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


def setup_descriptive_statistics_schema(cursor, artifacts_bucket):
    """Setup descriptive statistics schema and tables"""
    logger.info("Setting up descriptive statistics schema...")

    # Execute descriptive statistics schema SQL (no results expected)
    execute_sql_file(
        cursor,
        artifacts_bucket,
        "sql/descriptive_statistics_schema.sql",
        return_results=False,
    )
    logger.info("Descriptive statistics schema setup completed")


def get_table_data_from_rds(table_name):
    """Get table data from RDS and return as Spark DataFrame"""
    logger.info(f"Loading data from table: {table_name}")

    try:
        # Read directly from RDS using Spark JDBC
        df = (
            spark.read.format("jdbc")
            .option(
                "url", f"jdbc:postgresql://{args['DB_HOST']}:5432/{args['DB_NAME']}"
            )
            .option("dbtable", table_name)
            .option("user", args["DB_USER"])
            .option("password", args["DB_PASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        logger.info(f"Successfully loaded {df.count()} rows from {table_name}")
        return df

    except Exception as e:
        logger.error(f"Failed to load data from {table_name}: {str(e)}")
        raise


def calculate_descriptive_statistics(df, table_name):
    """Calculate descriptive statistics for all numeric columns in a DataFrame"""
    logger.info(f"Calculating descriptive statistics for table: {table_name}")

    # Get numeric columns (exclude ID columns)
    numeric_columns = []
    for field in df.schema.fields:
        if (
            field.dataType in [DoubleType(), IntegerType()]
            and not field.name.lower().endswith("_id")
            and field.name.lower() != "id"
        ):
            numeric_columns.append(field.name)

    logger.info(
        f"Found {len(numeric_columns)} numeric columns in {table_name}: {numeric_columns}"
    )

    statistics_results = []

    for column in numeric_columns:
        try:
            # Calculate statistics using Spark SQL
            stats_df = df.select(
                spark_min(col(column)).alias("min_val"),
                spark_max(col(column)).alias("max_val"),
                mean(col(column)).alias("mean_val"),
                stddev(col(column)).alias("std_val"),
                count(col(column)).alias("count_non_null"),
                count("*").alias("count_total"),
            )

            # Get the single row result
            stats_row = stats_df.collect()[0]

            # Calculate median using approximate percentile
            median_df = df.select(
                expr(f"percentile_approx({column}, 0.5)").alias("median_val")
            )
            median_val = median_df.collect()[0]["median_val"]

            # Store results
            stats_result = {
                "table_name": table_name,
                "column_name": column,
                "min_value": (
                    float(stats_row["min_val"])
                    if stats_row["min_val"] is not None
                    else None
                ),
                "max_value": (
                    float(stats_row["max_val"])
                    if stats_row["max_val"] is not None
                    else None
                ),
                "mean_value": (
                    float(stats_row["mean_val"])
                    if stats_row["mean_val"] is not None
                    else None
                ),
                "median_value": float(median_val) if median_val is not None else None,
                "std_deviation": (
                    float(stats_row["std_val"])
                    if stats_row["std_val"] is not None
                    else None
                ),
                "count_non_null": int(stats_row["count_non_null"]),
                "count_total": int(stats_row["count_total"]),
            }

            statistics_results.append(stats_result)
            logger.info(f"Calculated statistics for {table_name}.{column}")

        except Exception as e:
            logger.error(
                f"Failed to calculate statistics for {table_name}.{column}: {str(e)}"
            )
            continue

    return statistics_results


def save_statistics_to_database(statistics_results, cursor):
    """Save calculated statistics to the database"""
    logger.info(f"Saving {len(statistics_results)} statistics records to database...")

    try:
        # Clear existing statistics
        cursor.execute("TRUNCATE TABLE descriptive_statistics.column_statistics;")

        # Insert new statistics
        for stats in statistics_results:
            cursor.execute(
                """
                INSERT INTO descriptive_statistics.column_statistics 
                (table_name, column_name, min_value, max_value, mean_value, 
                 median_value, std_deviation, count_non_null, count_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    stats["table_name"],
                    stats["column_name"],
                    stats["min_value"],
                    stats["max_value"],
                    stats["mean_value"],
                    stats["median_value"],
                    stats["std_deviation"],
                    stats["count_non_null"],
                    stats["count_total"],
                ),
            )

        logger.info(f"Successfully saved {len(statistics_results)} statistics records")

    except Exception as e:
        logger.error(f"Failed to save statistics to database: {str(e)}")
        raise


def create_industry_exposure_extended(cursor):
    """Create industry_exposure_extended table with categories using PySpark logic + cursor insert"""
    logger.info("Creating industry_exposure_extended table with categories using PySpark...")
    
    try:
        # Load industry_exposure data using Spark
        df = get_table_data_from_rds("business_analytics.industry_exposure")
        
        # Calculate quantiles using PySpark
        logger.info("Calculating quantiles using PySpark...")
        
        # Calculate Q1 and Q3 for total_revenue and customer_count in one go
        quantiles_df = df.select(
            percentile_approx("total_revenue", 0.25).alias("q1_revenue"),
            percentile_approx("total_revenue", 0.75).alias("q3_revenue"),
            percentile_approx("customer_count", 0.25).alias("q1_customers"),
            percentile_approx("customer_count", 0.75).alias("q3_customers")
        ).collect()[0]
        
        q1_revenue = quantiles_df["q1_revenue"]
        q3_revenue = quantiles_df["q3_revenue"]
        q1_customers = quantiles_df["q1_customers"]
        q3_customers = quantiles_df["q3_customers"]
        
        logger.info(f"Quantiles calculated - Revenue Q1: {q1_revenue}, Q3: {q3_revenue}")
        logger.info(f"Quantiles calculated - Customers Q1: {q1_customers}, Q3: {q3_customers}")
        
        # Add categories using PySpark
        df_with_categories = df.withColumn(
            "total_revenue_category",
            when(col("total_revenue") <= q1_revenue, "Low")
            .when(col("total_revenue") <= q3_revenue, "Mid")
            .otherwise("High")
        ).withColumn(
            "customer_count_category",
            when(col("customer_count") <= q1_customers, "Low")
            .when(col("customer_count") <= q3_customers, "Mid")
            .otherwise("High")
        )
        
        # Convert to Pandas for cursor insert (like the statistics style)
        pandas_df = df_with_categories.toPandas()
        
        # Clear existing extended table
        cursor.execute("TRUNCATE TABLE descriptive_statistics.industry_exposure_extended;")
        
        # Insert data using cursor (same style as statistics)
        logger.info("Inserting categorized data into database...")
        for _, row in pandas_df.iterrows():
            cursor.execute("""
                INSERT INTO descriptive_statistics.industry_exposure_extended 
                (industry, total_revenue, customer_count, total_revenue_category, customer_count_category)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row["industry"],
                float(row["total_revenue"]),
                int(row["customer_count"]),
                row["total_revenue_category"],
                row["customer_count_category"]
            ))
        
        # Get count of inserted records
        cursor.execute("SELECT COUNT(*) FROM descriptive_statistics.industry_exposure_extended")
        count = cursor.fetchone()[0]
        
        logger.info(f"Successfully created industry_exposure_extended with {count} records using PySpark + cursor")
        
    except Exception as e:
        logger.error(f"Failed to create industry_exposure_extended: {str(e)}")
        raise


def analyze_industry_exposure():
    """Analyze industry_exposure table and calculate descriptive statistics with categories"""
    logger.info("Starting descriptive statistics analysis for industry_exposure table...")
    print("Starting descriptive statistics analysis for industry_exposure table...")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Setup descriptive statistics schema first
        artifacts_bucket = args["ARTIFACTS_BUCKET"]
        setup_descriptive_statistics_schema(cursor, artifacts_bucket)

        # Analyze industry_exposure table
        table_name = "business_analytics.industry_exposure"
        
        try:
            logger.info(f"Analyzing table: {table_name}")

            # Load table data directly with Spark
            df = get_table_data_from_rds(table_name)

            # Calculate statistics
            table_statistics = calculate_descriptive_statistics(df, table_name)

            # Save statistics to database
            if table_statistics:
                save_statistics_to_database(table_statistics, cursor)
                
                # Create industry_exposure_extended table with categories
                create_industry_exposure_extended(cursor)
                
                conn.commit()
                logger.info(
                    f"Descriptive statistics analysis completed successfully. Processed {len(table_statistics)} numeric columns."
                )
                print(
                    f"Descriptive statistics analysis completed successfully. Processed {len(table_statistics)} numeric columns."
                )
            else:
                logger.warning("No statistics were calculated")
                print("No statistics were calculated")

        except Exception as e:
            logger.error(f"Failed to analyze table {table_name}: {str(e)}")
            raise

    except Exception as e:
        conn.rollback()
        logger.error(f"Error in descriptive statistics analysis: {str(e)}")
        print(f"Error in descriptive statistics analysis: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
        logger.info("Database connection closed")


def main():
    """Main function to run the descriptive statistics job"""
    logger.info(f"Starting descriptive statistics job at {datetime.now()}")
    print(f"Starting descriptive statistics job at {datetime.now()}")

    try:
        # Analyze industry_exposure table and calculate descriptive statistics with categories
        analyze_industry_exposure()

        logger.info(
            f"Descriptive statistics job completed successfully at {datetime.now()}"
        )
        print(f"Descriptive statistics job completed successfully at {datetime.now()}")

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
