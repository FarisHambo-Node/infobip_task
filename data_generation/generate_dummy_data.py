import pandas as pd
import random
from datetime import datetime, timedelta

def generate_customers(num_customers=100):
    customers = []
    segments = ['Enterprise', 'SMB', 'Startup']
    industries = ['Technology', 'Finance', 'Healthcare']
    
    for i in range(num_customers):
        customer = {
            'customer_id': i + 1,
            'account_id': random.randint(1, 10),
            'customer_name': f'Customer {i+1}',
            'segment': random.choice(segments),
            'industry': random.choice(industries),
            'country': 'USA',
            'created_date': datetime.now().date()
        }
        customers.append(customer)
    
    return pd.DataFrame(customers)

def generate_channels():
    channels_data = [
        {'channel_id': 1, 'channel_name': 'SMS', 'channel_type': 'SMS', 'unit_price_eur': 0.05},
        {'channel_id': 2, 'channel_name': 'Email', 'channel_type': 'Email', 'unit_price_eur': 0.01},
        {'channel_id': 3, 'channel_name': 'Push', 'channel_type': 'Push', 'unit_price_eur': 0.02}
    ]
    return pd.DataFrame(channels_data)

def generate_traffic(num_days=30, num_customers=100):
    traffic_records = []
    record_id = 1
    
    for day in range(num_days):
        date = datetime.now().date() - timedelta(days=day)
        
        for _ in range(10):  # 10 records per day
            traffic_record = {
                'traffic_id': record_id,
                'send_date': date,
                'account_id': random.randint(1, 10),
                'customer_id': random.randint(1, num_customers),
                'channel_id': random.randint(1, 3),
                'interactions_count': random.randint(1, 1000),
                'revenue_eur': random.uniform(10, 500)
            }
            traffic_records.append(traffic_record)
            record_id += 1
    
    return pd.DataFrame(traffic_records)

def main():
    print("Generating dummy data...")
    
    customers_df = generate_customers()
    channels_df = generate_channels()
    traffic_df = generate_traffic()
    
    customers_df.to_csv('data/customers.csv', index=False)
    channels_df.to_csv('data/channels.csv', index=False)
    traffic_df.to_csv('data/traffic.csv', index=False)
    
    print("Data generation completed!")

if __name__ == "__main__":
    main()
