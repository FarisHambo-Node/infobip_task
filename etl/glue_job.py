import sys
import boto3
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

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
        ["JOB_NAME", "S3_BUCKET", "DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"],
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


def calculate_date_ranges():
    """Calculate date ranges for revenue calculations"""
    logger.info("Calculating date ranges...")
    current_date = datetime.now()
    logger.info(f"Current date: {current_date}")

    # Last month
    if current_date.month == 1:
        last_month_start = current_date.replace(
            year=current_date.year - 1, month=12, day=1
        )
    else:
        last_month_start = current_date.replace(month=current_date.month - 1, day=1)

    # Last quarter
    current_quarter = (current_date.month - 1) // 3 + 1
    if current_quarter == 1:
        last_quarter_start = current_date.replace(
            year=current_date.year - 1, month=10, day=1
        )
    else:
        last_quarter_start = current_date.replace(
            month=(current_quarter - 2) * 3 + 1, day=1
        )

    # Current quarter start
    current_quarter_start = current_date.replace(
        month=(current_quarter - 1) * 3 + 1, day=1
    )

    # Previous quarter start
    if current_quarter == 1:
        prev_quarter_start = current_date.replace(
            year=current_date.year - 1, month=10, day=1
        )
    else:
        prev_quarter_start = current_date.replace(
            month=(current_quarter - 2) * 3 + 1, day=1
        )

    date_ranges = {
        "last_month_start": last_month_start,
        "last_quarter_start": last_quarter_start,
        "current_quarter_start": current_quarter_start,
        "prev_quarter_start": prev_quarter_start,
        "mtd_start": current_date.replace(day=1),
        "ytd_start": current_date.replace(month=1, day=1),
    }

    logger.info(f"Date ranges calculated: {date_ranges}")
    return date_ranges


def main():
    try:
        logger.info("Starting ETL job...")
        print("Starting ETL job...")

        # Read data from S3
        s3_bucket = args["S3_BUCKET"]
        traffic_path = f"s3://{s3_bucket}/raw_data/traffic.csv"

        logger.info(f"Reading traffic data from {traffic_path}")
        print(f"Reading traffic data from {traffic_path}")

        try:
            traffic_df = spark.read.option("header", "true").csv(traffic_path)
            logger.info("Successfully read CSV file from S3")

            traffic_df = traffic_df.withColumn(
                "send_date", traffic_df["send_date"].cast("date")
            )
            traffic_df = traffic_df.withColumn(
                "revenue_eur", traffic_df["revenue_eur"].cast("double")
            )
            logger.info("Successfully cast columns to proper data types")

        except Exception as e:
            logger.error(f"Failed to read or process CSV file: {str(e)}")
            raise

        # Convert to Pandas for easier date calculations
        logger.info("Converting Spark DataFrame to Pandas...")
        try:
            traffic_pandas = traffic_df.toPandas()
            traffic_pandas["send_date"] = pd.to_datetime(traffic_pandas["send_date"])
            logger.info(
                f"Successfully converted to Pandas. Loaded {len(traffic_pandas)} traffic records"
            )
            print(f"Loaded {len(traffic_pandas)} traffic records")
        except Exception as e:
            logger.error(f"Failed to convert to Pandas: {str(e)}")
            raise

        # Calculate date ranges
        logger.info("Calculating date ranges...")
        date_ranges = calculate_date_ranges()

        # Calculate revenue metrics using pandas aggregation
        logger.info("Calculating revenue metrics using aggregation...")
        print("Calculating revenue metrics using aggregation...")

        # Create period flags for efficient filtering
        traffic_pandas['is_last_month'] = (
            (traffic_pandas["send_date"] >= date_ranges["last_month_start"]) &
            (traffic_pandas["send_date"] < date_ranges["mtd_start"])
        )
        
        traffic_pandas['is_last_quarter'] = (
            (traffic_pandas["send_date"] >= date_ranges["last_quarter_start"]) &
            (traffic_pandas["send_date"] < date_ranges["current_quarter_start"])
        )
        
        traffic_pandas['is_mtd'] = (
            traffic_pandas["send_date"] >= date_ranges["mtd_start"]
        )
        
        traffic_pandas['is_ytd'] = (
            traffic_pandas["send_date"] >= date_ranges["ytd_start"]
        )
        
        traffic_pandas['is_current_quarter'] = (
            traffic_pandas["send_date"] >= date_ranges["current_quarter_start"]
        )
        
        traffic_pandas['is_prev_quarter'] = (
            (traffic_pandas["send_date"] >= date_ranges["prev_quarter_start"]) &
            (traffic_pandas["send_date"] < date_ranges["current_quarter_start"])
        )

        # Aggregate revenue by customer using lambda functions
        revenue_metrics = traffic_pandas.groupby('customer_id').agg({
            'revenue_eur': [
                ('revenue_last_month', lambda x: x[traffic_pandas.loc[x.index, 'is_last_month']].sum()),
                ('revenue_last_quarter', lambda x: x[traffic_pandas.loc[x.index, 'is_last_quarter']].sum()),
                ('revenue_mtd', lambda x: x[traffic_pandas.loc[x.index, 'is_mtd']].sum()),
                ('revenue_ytd', lambda x: x[traffic_pandas.loc[x.index, 'is_ytd']].sum()),
                ('revenue_current_quarter', lambda x: x[traffic_pandas.loc[x.index, 'is_current_quarter']].sum()),
                ('revenue_prev_quarter', lambda x: x[traffic_pandas.loc[x.index, 'is_prev_quarter']].sum())
            ]
        }).round(2)

        # Flatten column names
        revenue_metrics.columns = revenue_metrics.columns.droplevel(0)
        revenue_metrics = revenue_metrics.reset_index()

        # Calculate quarter-over-quarter percentage
        revenue_metrics['revenue_increase_pct_qoq'] = revenue_metrics.apply(
            lambda row: round(
                ((row['revenue_current_quarter'] - row['revenue_prev_quarter']) / row['revenue_prev_quarter'] * 100)
                if row['revenue_prev_quarter'] > 0 else 0.0, 2
            ), axis=1
        )

        # Drop temporary columns and convert to list of dictionaries
        revenue_metrics = revenue_metrics.drop(['revenue_current_quarter', 'revenue_prev_quarter'], axis=1)
        results = revenue_metrics.to_dict('records')

        logger.info(f"Calculated revenue metrics for {len(results)} customers using vectorized operations")
        print(f"Calculated revenue metrics for {len(results)} customers using vectorized operations")

        # Connect to RDS and upsert data
        logger.info("Connecting to RDS database...")
        try:
            conn = psycopg2.connect(
                host=args["DB_HOST"],
                database=args["DB_NAME"],
                user=args["DB_USER"],
                password=args["DB_PASSWORD"],
            )
            logger.info("Successfully connected to RDS database")

            cursor = conn.cursor()
            logger.info("Database cursor created")
        except Exception as e:
            logger.error(f"Failed to connect to RDS database: {str(e)}")
            raise

        # Upsert each record
        logger.info("Starting database upsert operations...")
        upsert_sql = """
        INSERT INTO customers_revenue_by_period (
            customer_id, 
            revenue_last_month, 
            revenue_last_quarter, 
            revenue_mtd, 
            revenue_ytd, 
            revenue_increase_pct_qoq,
            last_updated
        ) VALUES (
            %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (customer_id) 
        DO UPDATE SET
            revenue_last_month = EXCLUDED.revenue_last_month,
            revenue_last_quarter = EXCLUDED.revenue_last_quarter,
            revenue_mtd = EXCLUDED.revenue_mtd,
            revenue_ytd = EXCLUDED.revenue_ytd,
            revenue_increase_pct_qoq = EXCLUDED.revenue_increase_pct_qoq,
            last_updated = CURRENT_TIMESTAMP;
        """

        try:
            for i, result in enumerate(results):
                if i % 100 == 0:  # Log every 100 records
                    logger.info(f"Upserting record {i+1}/{len(results)}")

                cursor.execute(
                    upsert_sql,
                    (
                        result["customer_id"],
                        result["revenue_last_month"],
                        result["revenue_last_quarter"],
                        result["revenue_mtd"],
                        result["revenue_ytd"],
                        result["revenue_increase_pct_qoq"],
                    ),
                )

            logger.info("Committing database transaction...")
            conn.commit()
            logger.info("Database transaction committed successfully")

        except Exception as e:
            logger.error(f"Failed to upsert records to database: {str(e)}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
            logger.info("Database connection closed")

        logger.info(f"Successfully upserted {len(results)} records to RDS")
        print(f"Successfully upserted {len(results)} records to RDS")

    except Exception as e:
        logger.error(f"Error in ETL job: {str(e)}")
        print(f"Error in ETL job: {str(e)}")
        raise

    finally:
        logger.info("Committing Glue job...")
        job.commit()
        logger.info("Glue job committed successfully")


if __name__ == "__main__":
    logger.info("Starting Glue job execution...")
    main()
    logger.info("Glue job execution completed successfully")
