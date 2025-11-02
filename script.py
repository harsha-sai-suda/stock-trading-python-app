import requests
import os
import csv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from datetime import datetime

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000
DS = '02-11-2025'

def write_tickers_to_csv(tickers, example_ticker, csv_file="tickers.csv"):
    """Write a list of ticker to a CSV file using the schema from example_ticker.
    
    Args:
        tickers (list[dict]): List of ticker dictionaries from Polygon API
        example_ticker (dict): Example ticker dictionary defining the schema
        csv_file (str, optional): Path to output CSV file. Defaults to "tickers.csv"
    """
    csv_columns = list(example_ticker.keys())
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for ticker in tickers:
            # Ensure all keys exist, fill missing with empty string
            row = {key: ticker.get(key, "") for key in csv_columns}
            writer.writerow(row)
    
    print(f'Wrote {len(tickers)} tickers to {csv_file}')

# Snowflake configuration
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'role': os.getenv('SNOWFLAKE_ROLE')
}

def load_tickers_to_snowflake(tickers, example_ticker):
    """Load ticker data into Snowflake table.
    
    Args:
        tickers (list[dict]): List of ticker dictionaries from Polygon API
        example_ticker (dict): Example ticker dictionary defining the schema
    """
    def get_snowflake_type(column_name, value):
        if column_name.lower() in ['last_updated_utc']:
            return 'TIMESTAMP'
        elif isinstance(value, bool):
            return 'BOOLEAN'
        else:
            return 'VARCHAR'

    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

        # Create table if not exists using example_ticker schema
        columns = [
            f'"{col}" {get_snowflake_type(col, val)}' 
            for col, val in example_ticker.items()
        ]
        
        columns_sql = ',\n            '.join(columns)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS stock_tickers (
            {columns_sql}
        )
        """
        
        conn.cursor().execute(create_table_sql)
        
         # Convert tickers to pandas DataFrame (table)
        df = pd.DataFrame(tickers)
        
        # Write DataFrame to Snowflake
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            table_name='STOCK_TICKERS',
            schema=SNOWFLAKE_CONFIG['schema'],
            database=SNOWFLAKE_CONFIG['database'],
            df=df
        )
        
        print(f'Successfully loaded {nrows} rows into Snowflake table stock_tickers')
        
    except Exception as e:
        print(f'Error loading data to Snowflake: {str(e)}')
    
    finally:
        if 'conn' in locals():
            conn.close()

def run_stock_job():
    DS=datetime.now().strftime('%d-%m-%Y')
    # GET data from Polygon.io API using GET request
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    tickers = []

    data = response.json()
    for ticker in data['results']:
        ticker['ds'] = DS
        tickers.append(ticker)

    # Handle paginated API responses
    while 'next_url' in data:
        print('requesting next page...', data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        if 'error' in data:
            print('error: ', data['error'])
            break
        #print(data)
        for ticker in data['results']:
            ticker['ds'] = DS
            tickers.append(ticker)

    print(len(tickers))

    # Example ticker data defining the schema
    example_ticker = {'ticker': 'HSBH',
       'name': 'HSBC Holdings plc ADRhedged',
       'market': 'stocks', 
       'locale': 'us', 
       'primary_exchange': 'ARCX', 
       'type': 'ETS', 
       'active': True, 
       'currency_name': 'usd', 
       'cik': '0000044760',
       'composite_figi': 'BBG01Q37TG78', 
       'share_class_figi': 'BBG01Q37THL0', 
       'last_updated_utc': '2025-10-12T06:05:08.375181062Z',
       'ds': '02-11-2025'}
    
    # Write tickers to CSV using the schema from example_ticker
    write_tickers_to_csv(tickers, example_ticker)
    
    # Load data into Snowflake using same schema as CSV
    load_tickers_to_snowflake(tickers, example_ticker)

if __name__ == "__main__":
    run_stock_job()