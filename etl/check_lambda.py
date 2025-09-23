import json
import os
from datetime import datetime
import pg8000


def lambda_handler(event, context):
    """Simple Lambda to check if customers_revenue_by_period table was updated this month"""

    try:
        # Connect to RDS using pg8000 (pure Python, no compilation needed)
        conn = pg8000.connect(
            host=os.environ["DB_HOST"],
            database=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            ssl_context=True,  # Enable SSL connection
        )

        cursor = conn.cursor()

        # Check if table has records updated this month
        current_month = datetime.now().strftime("%Y-%m")

        cursor.execute(
            """
            SELECT COUNT(*) 
            FROM customers_revenue_by_period 
            WHERE DATE_TRUNC('month', last_updated) = DATE_TRUNC('month', CURRENT_DATE)
        """
        )

        count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        if count > 0:
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": f"Table updated this month with {count} records",
                        "success": True,
                    }
                ),
            }
        else:
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {"message": "No records updated this month", "success": False}
                ),
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "success": False}),
        }
