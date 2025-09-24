# Infobip Data Engineer Task

Data processing system on AWS with ETL pipelines, business analytics, and statistical analysis.

## Architecture

### Components
- **Base Infrastructure**: RDS PostgreSQL, S3 buckets, VPC, IAM roles
- **ETL Pipeline**: AWS Glue job for data loading and Lambda for validation
- **Analytics Pipeline**: Business queries and statistical analysis
- **Orchestration**: Step Functions for workflow execution and EventBridge scheduling

### Data Flow
1. Data Generation → CSV files (customers, traffic, channels)
2. ETL Pipeline → Data loading into PostgreSQL
3. Analytics Pipeline → Business queries and statistics
4. Orchestration → Automated execution

## Deployment

### Prerequisites
- AWS CLI configured
- Terraform >= 1.0
- Python 3.8+

### Deployment Order
Deploy in this order due to dependencies:

#### 1. Data Generation
```bash
cd data_generation
python generate_dummy_data.py
```

#### 2. Base Infrastructure
```bash
cd infrastructure/base/terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values
terraform init && terraform apply
```

#### 3. ETL Infrastructure
```bash
cd infrastructure/etl/terraform
cp terraform.tfvars.example terraform.tfvars
# Update with Base infrastructure outputs
terraform init && terraform apply
```

#### 4. Analytics Infrastructure
```bash
cd infrastructure/analytics/terraform
cp terraform.tfvars.example terraform.tfvars
# Update with Base infrastructure outputs
terraform init && terraform apply
```

#### 5. Orchestration
```bash
cd infrastructure/orchestration/terraform
cp terraform.tfvars.example terraform.tfvars
# Update with ETL and Analytics outputs
terraform init && terraform apply
```

### Configuration
Each infrastructure module includes a `terraform.tfvars.example` file. Copy to `terraform.tfvars` and update with your values.

## Analytics Features

### Business Queries
1. Industry Exposure: Revenue analysis by industry
2. Segment Analysis: Client distribution and revenue share
3. Recent Customers: Segment changes within 12 months
4. Top Customers: Top 20 customers by revenue
5. Monthly Active Customers: Top 10% customers with 12-month revenue

### Statistical Analysis
Analysis of all numeric columns across tables:
- Min, Max, Mean, Median, Standard Deviation
- Count statistics and data quality metrics

## Project Structure

```
├── analytics/                 # Glue jobs and SQL queries
├── data_generation/          # CSV data generation scripts
├── database/                 # Database schema definitions
├── etl/                      # ETL pipeline components
├── infrastructure/           # Terraform infrastructure code
│   ├── analytics/           # Analytics infrastructure
│   ├── base/                # Base infrastructure (RDS, S3, VPC)
│   ├── etl/                 # ETL infrastructure
│   └── orchestration/       # Step Functions and scheduling
└── requirements.txt         # Python dependencies
```

## Branching Strategy

- `main`: Production-ready code
- `develop`: Integration branch for features
