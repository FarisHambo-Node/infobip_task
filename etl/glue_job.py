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
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Glue job parameters
logger.info("Getting job parameters...")
try:
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'S3_BUCKET',
        'DB_HOST',
        'DB_NAME', 
        'DB_USER',
        'DB_PASSWORD'
    ])
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
    job.init(args['JOB_NAME'], args)
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
        last_month_start = current_date.replace(year=current_date.year-1, month=12, day=1)
    else:
        last_month_start = current_date.replace(month=current_date.month-1, day=1)
    
    # Last quarter
    current_quarter = (current_date.month - 1) // 3 + 1
    if current_quarter == 1:
        last_quarter_start = current_date.replace(year=current_date.year-1, month=10, day=1)
    else:
        last_quarter_start = current_date.replace(month=(current_quarter-2)*3+1, day=1)
    
    # Current quarter start
    current_quarter_start = current_date.replace(month=(current_quarter-1)*3+1, day=1)
    
    # Previous quarter start
    if current_quarter == 1:
        prev_quarter_start = current_date.replace(year=current_date.year-1, month=10, day=1)
    else:
        prev_quarter_start = current_date.replace(month=(current_quarter-2)*3+1, day=1)
    
    date_ranges = {
        'last_month_start': last_month_start,
        'last_quarter_start': last_quarter_start,
        'current_quarter_start': current_quarter_start,
        'prev_quarter_start': prev_quarter_start,
        'mtd_start': current_date.replace(day=1),
        'ytd_start': current_date.replace(month=1, day=1)
    }
    
    logger.info(f"Date ranges calculated: {date_ranges}")
    return date_ranges

def main():
    try:
        logger.info("Starting ETL job...")
        print("Starting ETL job...")
        
        # Read data from S3
        s3_bucket = args['S3_BUCKET']
        traffic_path = f"s3://{s3_bucket}/raw_data/traffic.csv"
        
        logger.info(f"Reading traffic data from {traffic_path}")
        print(f"Reading traffic data from {traffic_path}")
        
        try:
            traffic_df = spark.read.option("header", "true").csv(traffic_path)
            logger.info("Successfully read CSV file from S3")
            
            traffic_df = traffic_df.withColumn("send_date", traffic_df["send_date"].cast("date"))
            traffic_df = traffic_df.withColumn("revenue_eur", traffic_df["revenue_eur"].cast("double"))
            logger.info("Successfully cast columns to proper data types")
            
        except Exception as e:
            logger.error(f"Failed to read or process CSV file: {str(e)}")
            raise
        
        # Convert to Pandas for easier date calculations
        logger.info("Converting Spark DataFrame to Pandas...")
        try:
            traffic_pandas = traffic_df.toPandas()
            traffic_pandas['send_date'] = pd.to_datetime(traffic_pandas['send_date'])
            logger.info(f"Successfully converted to Pandas. Loaded {len(traffic_pandas)} traffic records")
            print(f"Loaded {len(traffic_pandas)} traffic records")
        except Exception as e:
            logger.error(f"Failed to convert to Pandas: {str(e)}")
            raise
        
        # Calculate date ranges
        logger.info("Calculating date ranges...")
        date_ranges = calculate_date_ranges()
        
        # Get unique customers
        logger.info("Getting unique customers...")
        customers = traffic_pandas['customer_id'].unique()
        results = []
        
        logger.info(f"Processing {len(customers)} customers...")
        print(f"Processing {len(customers)} customers...")
        
        for i, customer_id in enumerate(customers):
            if i % 100 == 0:  # Log every 100 customers
                logger.info(f"Processing customer {i+1}/{len(customers)}: {customer_id}")
            customer_traffic = traffic_pandas[traffic_pandas['customer_id'] == customer_id]
            
            # Calculate revenue for different periods
            revenue_last_month = customer_traffic[
                (customer_traffic['send_date'] >= date_ranges['last_month_start']) & 
                (customer_traffic['send_date'] < date_ranges['mtd_start'])
            ]['revenue_eur'].sum()
            
            revenue_last_quarter = customer_traffic[
                (customer_traffic['send_date'] >= date_ranges['last_quarter_start']) & 
                (customer_traffic['send_date'] < date_ranges['current_quarter_start'])
            ]['revenue_eur'].sum()
            
            revenue_mtd = customer_traffic[
                customer_traffic['send_date'] >= date_ranges['mtd_start']
            ]['revenue_eur'].sum()
            
            revenue_ytd = customer_traffic[
                customer_traffic['send_date'] >= date_ranges['ytd_start']
            ]['revenue_eur'].sum()
            
            # Calculate quarter-over-quarter percentage
            current_quarter_revenue = customer_traffic[
                customer_traffic['send_date'] >= date_ranges['current_quarter_start']
            ]['revenue_eur'].sum()
            
            prev_quarter_revenue = customer_traffic[
                (customer_traffic['send_date'] >= date_ranges['prev_quarter_start']) & 
                (customer_traffic['send_date'] < date_ranges['current_quarter_start'])
            ]['revenue_eur'].sum()
            
            if prev_quarter_revenue > 0:
                revenue_increase_pct_qoq = ((current_quarter_revenue - prev_quarter_revenue) / prev_quarter_revenue) * 100
            else:
                revenue_increase_pct_qoq = 0.0
            
            results.append({
                'customer_id': int(customer_id),
                'revenue_last_month': round(revenue_last_month, 2),
                'revenue_last_quarter': round(revenue_last_quarter, 2),
                'revenue_mtd': round(revenue_mtd, 2),
                'revenue_ytd': round(revenue_ytd, 2),
                'revenue_increase_pct_qoq': round(revenue_increase_pct_qoq, 2)
            })
        
        logger.info(f"Calculated revenue metrics for {len(results)} customers")
        print(f"Calculated revenue metrics for {len(results)} customers")
        
        # Connect to RDS and upsert data
        logger.info("Connecting to RDS database...")
        try:
            conn = psycopg2.connect(
                host=args['DB_HOST'],
                database=args['DB_NAME'],
                user=args['DB_USER'],
                password=args['DB_PASSWORD']
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
                
                cursor.execute(upsert_sql, (
                    result['customer_id'],
                    result['revenue_last_month'],
                    result['revenue_last_quarter'],
                    result['revenue_mtd'],
                    result['revenue_ytd'],
                    result['revenue_increase_pct_qoq']
                ))
            
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
