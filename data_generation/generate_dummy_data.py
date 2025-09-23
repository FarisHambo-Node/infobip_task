#!/usr/bin/env python3
"""
Data Engineering Task - Dummy Data Generator
Generates dummy data for the 3 required tables and uploads to S3
"""

import os
import random
import logging
from datetime import datetime, timedelta
from io import StringIO

import boto3
import pandas as pd
from faker import Faker
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker for generating realistic data
fake = Faker()


class DummyDataGenerator:
    def __init__(self, s3_bucket_name=None, aws_region="eu-north-1"):
        self.s3_bucket_name = s3_bucket_name
        self.aws_region = aws_region
        self.s3_client = None

        if s3_bucket_name:
            try:
                self.s3_client = boto3.client("s3", region_name=aws_region)
                logger.info(f"S3 client initialized for bucket: {s3_bucket_name}")
            except Exception as e:
                logger.error(f"Failed to initialize S3 client: {str(e)}")
                raise

    def generate_customers(self, num_customers=100):
        """Generate dummy customer data"""
        logger.info("Generating customers...")
        print(f"Generating {num_customers} customers...")

        segments = ["Enterprise", "SMB", "Startup", "Mid-Market"]
        industries = [
            "Technology",
            "Finance",
            "Healthcare",
            "Retail",
            "Manufacturing",
            "Education",
            "Government",
            "Media",
            "Telecommunications",
            "Energy",
        ]
        countries = [
            "United States",
            "United Kingdom",
            "Germany",
            "France",
            "Italy",
            "Spain",
            "Netherlands",
            "Sweden",
            "Norway",
            "Denmark",
        ]

        customers = []
        for i in range(num_customers):
            customer = {
                "customer_id": i + 1,
                "account_id": random.randint(1, 50),  # 50 different accounts
                "customer_name": fake.company(),
                "segment": random.choice(segments),
                "industry": random.choice(industries),
                "country": random.choice(countries),
                "created_date": fake.date_between(start_date="-2y", end_date="today"),
            }
            customers.append(customer)

        customers_df = pd.DataFrame(customers)
        logger.info(f"Generated {len(customers_df)} customers")
        return customers_df

    def generate_channels(self):
        """Generate dummy channel data"""
        logger.info("Generating channels...")
        print("Generating channels...")

        channels_data = [
            {
                "channel_id": 1,
                "channel_name": "SMS",
                "channel_type": "SMS",
                "unit_price_eur": 0.05,
            },
            {
                "channel_id": 2,
                "channel_name": "Email",
                "channel_type": "Email",
                "unit_price_eur": 0.01,
            },
            {
                "channel_id": 3,
                "channel_name": "Push Notification",
                "channel_type": "Push",
                "unit_price_eur": 0.02,
            },
            {
                "channel_id": 4,
                "channel_name": "Voice Call",
                "channel_type": "Voice",
                "unit_price_eur": 0.15,
            },
            {
                "channel_id": 5,
                "channel_name": "WhatsApp",
                "channel_type": "WhatsApp",
                "unit_price_eur": 0.08,
            },
            {
                "channel_id": 6,
                "channel_name": "Viber",
                "channel_type": "Viber",
                "unit_price_eur": 0.06,
            },
            {
                "channel_id": 7,
                "channel_name": "Telegram",
                "channel_type": "Telegram",
                "unit_price_eur": 0.04,
            },
            {
                "channel_id": 8,
                "channel_name": "Facebook Messenger",
                "channel_type": "Messenger",
                "unit_price_eur": 0.07,
            },
        ]

        channels_df = pd.DataFrame(channels_data)
        logger.info(f"Generated {len(channels_df)} channels")
        return channels_df

    def generate_traffic(self, num_days=365, num_customers=100):
        """Generate dummy traffic data for the last 12 months"""
        logger.info("Generating traffic...")
        print(f"Generating traffic data for {num_days} days...")

        # Generate date range for the last 12 months
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=num_days)
        date_range = pd.date_range(start=start_date, end=end_date, freq="D")

        traffic_records = []
        record_id = 1

        for date in date_range:
            # Generate 10-50 traffic records per day
            daily_records = random.randint(10, 50)

            for _ in range(daily_records):
                customer_id = random.randint(1, num_customers)
                channel_id = random.randint(1, 8)  # 8 channels
                account_id = random.randint(1, 50)

                # Generate interactions count (1-10000)
                interactions_count = random.randint(1, 10000)

                # Calculate revenue based on channel pricing and interactions
                # Add some randomness to pricing
                base_prices = [0.05, 0.01, 0.02, 0.15, 0.08, 0.06, 0.04, 0.07]
                unit_price = base_prices[channel_id - 1] * random.uniform(0.8, 1.2)
                revenue_eur = round(interactions_count * unit_price, 2)

                traffic_record = {
                    "traffic_id": record_id,
                    "send_date": date.date(),
                    "account_id": account_id,
                    "customer_id": customer_id,
                    "channel_id": channel_id,
                    "interactions_count": interactions_count,
                    "revenue_eur": revenue_eur,
                }

                traffic_records.append(traffic_record)
                record_id += 1

        traffic_df = pd.DataFrame(traffic_records)
        logger.info(f"Generated {len(traffic_df)} traffic records")
        return traffic_df

    def upload_to_s3(self, df, table_name, file_format="csv"):
        """Upload DataFrame to S3"""
        if not self.s3_client:
            print(f"No S3 client configured. Saving {table_name} locally...")
            df.to_csv(f"data/{table_name}.csv", index=False)
            return

        logger.info(f"Uploading {table_name} to S3...")
        print(f"Uploading {table_name} to S3...")

        try:
            if file_format == "csv":
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                key = f"raw_data/{table_name}.csv"

                self.s3_client.put_object(
                    Bucket=self.s3_bucket_name,
                    Key=key,
                    Body=csv_buffer.getvalue(),
                    ContentType="text/csv",
                )
            elif file_format == "json":
                json_data = df.to_json(orient="records", date_format="iso")
                key = f"raw_data/{table_name}.json"

                self.s3_client.put_object(
                    Bucket=self.s3_bucket_name,
                    Key=key,
                    Body=json_data,
                    ContentType="application/json",
                )

            logger.info(f"Uploaded {table_name} to S3")
            print(f"Successfully uploaded {table_name} to s3://{self.s3_bucket_name}/{key}")
            
        except Exception as e:
            logger.error(f"Upload failed: {table_name}")
            raise

    def generate_all_data(self, num_customers=100, num_days=365, upload_to_s3=True):
        """Generate all dummy data and optionally upload to S3"""
        logger.info("Starting data generation...")
        print("Starting dummy data generation...")

        try:
            # Create data directory if it doesn't exist
            os.makedirs("data", exist_ok=True)

            # Generate data
            customers_df = self.generate_customers(num_customers)
            channels_df = self.generate_channels()
            traffic_df = self.generate_traffic(num_days, num_customers)

            # Save locally
            logger.info("Saving data locally...")
            customers_df.to_csv("data/customers.csv", index=False)
            channels_df.to_csv("data/channels.csv", index=False)
            traffic_df.to_csv("data/traffic.csv", index=False)

            print(f"Generated data summary:")
            print(f"- Customers: {len(customers_df)} records")
            print(f"- Channels: {len(channels_df)} records")
            print(f"- Traffic: {len(traffic_df)} records")

            # Upload to S3 if configured
            if upload_to_s3 and self.s3_bucket_name:
                logger.info("Uploading to S3...")
                self.upload_to_s3(customers_df, "customers")
                self.upload_to_s3(channels_df, "channels")
                self.upload_to_s3(traffic_df, "traffic")

            logger.info("✅ Data generation completed successfully")
            return customers_df, channels_df, traffic_df

        except Exception as e:
            logger.error(f"❌ Data generation failed: {str(e)}")
            raise


def main():
    """Main function to run the data generation"""
    try:
        # You can set your S3 bucket name here or via environment variable
        s3_bucket_name = os.getenv("S3_BUCKET_NAME")

        # Initialize generator
        generator = DummyDataGenerator(s3_bucket_name=s3_bucket_name)

        # Generate data
        customers_df, channels_df, traffic_df = generator.generate_all_data(
            num_customers=100, num_days=365, upload_to_s3=bool(s3_bucket_name)
        )

        print("\nData generation completed!")
        print("Files saved to ./data/ directory")

        if s3_bucket_name:
            print(f"Files also uploaded to S3 bucket: {s3_bucket_name}")

    except Exception as e:
        print(f"\nError: {str(e)}")
        raise


if __name__ == "__main__":
    main()
