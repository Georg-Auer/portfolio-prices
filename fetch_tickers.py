#!/usr/bin/env python3
"""
Fetch ticker list from Databricks table using REST API
Saves tickers to tickers.json for use by fetch_prices.py
"""

import os
import requests
import json
import time

# Get credentials from environment variables (set in GitHub Secrets)
DATABRICKS_HOST = os.environ['DATABRICKS_HOST']  # e.g., https://adb-1234567890123456.7.azuredatabricks.net
DATABRICKS_TOKEN = os.environ['DATABRICKS_TOKEN']

# Configuration
CATALOG = 'workspace'
SCHEMA = 'portfolio'
TABLE = 'isin_ticker_mapping'

print(f"Fetching ticker list from Databricks table: {CATALOG}.{SCHEMA}.{TABLE}\n")

# Use Databricks SQL Statement Execution API
url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# SQL query to get tickers
sql_query = f"""
SELECT 
    security_isin,
    security_name,
    ticker_symbol
FROM {CATALOG}.{SCHEMA}.{TABLE}
WHERE ticker_symbol IS NOT NULL
ORDER BY security_name
"""

payload = {
    "statement": sql_query,
    "warehouse_id": None,  # Will use serverless SQL
    "wait_timeout": "30s"
}

try:
    # Execute query
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    
    result = response.json()
    
    # Check if we need to poll for results
    if result.get('status', {}).get('state') == 'PENDING':
        statement_id = result['statement_id']
        status_url = f"{url}/{statement_id}"
        
        # Poll for completion
        for _ in range(30):  # 30 attempts, 1 second apart = 30 seconds max
            time.sleep(1)
            status_response = requests.get(status_url, headers=headers)
            status_response.raise_for_status()
            result = status_response.json()
            
            state = result.get('status', {}).get('state')
            if state == 'SUCCEEDED':
                break
            elif state == 'FAILED':
                raise Exception(f"Query failed: {result.get('status', {}).get('error')}")
    
    # Extract results
    if result.get('status', {}).get('state') == 'SUCCEEDED':
        manifest = result.get('result', {}).get('data_array', [])
        
        # Parse ticker data
        tickers = []
        for row in manifest:
            if len(row) >= 3:
                tickers.append({
                    'isin': row[0],
                    'name': row[1],
                    'ticker': row[2]
                })
        
        print(f"✓ Found {len(tickers)} tickers")
        
        # Save to JSON file
        with open('tickers.json', 'w') as f:
            json.dump(tickers, f, indent=2)
        
        print(f"✓ Saved tickers to tickers.json")
        
        # Print sample
        print("\nSample tickers:")
        for ticker in tickers[:5]:
            print(f"  {ticker['ticker']}: {ticker['name']}")
    else:
        raise Exception(f"Unexpected query state: {result.get('status', {}).get('state')}")
        
except Exception as e:
    print(f"Error fetching tickers from Databricks: {str(e)}")
    raise