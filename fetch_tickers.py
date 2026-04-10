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
DATABRICKS_HOST = os.environ['DATABRICKS_HOST'].rstrip('/')  # Remove trailing slash if present
DATABRICKS_TOKEN = os.environ['DATABRICKS_TOKEN']
DATABRICKS_WAREHOUSE_ID = os.environ['DATABRICKS_WAREHOUSE_ID']

# Configuration
CATALOG = 'workspace'
SCHEMA = 'portfolio'
TABLE = 'isin_ticker_mapping'

print(f"Fetching ticker list from Databricks table: {CATALOG}.{SCHEMA}.{TABLE}")
print(f"Using warehouse: {DATABRICKS_WAREHOUSE_ID}")
print(f"Host: {DATABRICKS_HOST}\n")

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
    "warehouse_id": DATABRICKS_WAREHOUSE_ID,
    "wait_timeout": "30s"
}

try:
    # Execute query
    print(f"Sending request to: {url}")
    response = requests.post(url, headers=headers, json=payload)
    
    # Print response details before raising error
    print(f"Response status code: {response.status_code}")
    if response.status_code != 200:
        print(f"Response body: {response.text}")
    
    response.raise_for_status()
    
    result = response.json()
    
    # Check if we need to poll for results
    if result.get('status', {}).get('state') == 'PENDING':
        statement_id = result['statement_id']
        status_url = f"{url}/{statement_id}"
        
        print(f"Query is pending, polling for results...")
        
        # Poll for completion
        for i in range(60):  # 60 attempts, 2 seconds apart = 2 minutes max
            time.sleep(2)
            status_response = requests.get(status_url, headers=headers)
            status_response.raise_for_status()
            result = status_response.json()
            
            state = result.get('status', {}).get('state')
            print(f"  Attempt {i+1}: {state}")
            
            if state == 'SUCCEEDED':
                break
            elif state == 'FAILED':
                error_msg = result.get('status', {}).get('error', {})
                raise Exception(f"Query failed: {error_msg}")
    
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
    print(f"\nError fetching tickers from Databricks: {str(e)}")
    raise
