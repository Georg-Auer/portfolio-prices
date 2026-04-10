#!/usr/bin/env python3
"""
Automatically sync new securities and populate ticker symbols.
1. Syncs new securities from gold_current_holdings to isin_ticker_mapping
2. Looks up ticker symbols via OpenFIGI API for securities with NULL ticker
3. Updates the Databricks mapping table

This eliminates the need for manual syncing or UPDATE statements.
"""

import os
import requests
import json
import time

# Get credentials from environment variables (set in GitHub Secrets)
DATABRICKS_HOST = os.environ['DATABRICKS_HOST'].rstrip('/')  # Remove trailing slash
DATABRICKS_TOKEN = os.environ['DATABRICKS_TOKEN']
DATABRICKS_WAREHOUSE_ID = os.environ['DATABRICKS_WAREHOUSE_ID']

# Configuration
CATALOG = 'workspace'
SCHEMA = 'portfolio'
MAPPING_TABLE = 'isin_ticker_mapping'
HOLDINGS_TABLE = 'gold_current_holdings'
OPENFIGI_URL = 'https://api.openfigi.com/v3/mapping'

print("=" * 80)
print("Automated Ticker Management (Sync + Populate)")
print("=" * 80)
print(f"Mapping table: {CATALOG}.{SCHEMA}.{MAPPING_TABLE}")
print(f"Holdings table: {CATALOG}.{SCHEMA}.{HOLDINGS_TABLE}")
print(f"Warehouse: {DATABRICKS_WAREHOUSE_ID}")
print(f"Host: {DATABRICKS_HOST}\n")

def execute_sql(sql_query, description="Executing query"):
    """Execute SQL query on Databricks and return results"""
    url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "statement": sql_query,
        "warehouse_id": DATABRICKS_WAREHOUSE_ID,
        "wait_timeout": "30s"
    }
    
    print(f"{description}...")
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code != 200:
        print(f"✗ Error: {response.status_code}")
        print(f"Response: {response.text}")
        response.raise_for_status()
    
    result = response.json()
    
    # Poll for completion if needed
    if result.get('status', {}).get('state') == 'PENDING':
        statement_id = result['statement_id']
        status_url = f"{url}/{statement_id}"
        
        for i in range(60):  # Max 2 minutes
            time.sleep(2)
            status_response = requests.get(status_url, headers=headers)
            status_response.raise_for_status()
            result = status_response.json()
            
            state = result.get('status', {}).get('state')
            if state == 'SUCCEEDED':
                break
            elif state == 'FAILED':
                error_msg = result.get('status', {}).get('error', {})
                raise Exception(f"Query failed: {error_msg}")
    
    if result.get('status', {}).get('state') != 'SUCCEEDED':
        raise Exception(f"Unexpected query state: {result.get('status', {}).get('state')}")
    
    return result

def lookup_ticker_openfigi(isin):
    """Lookup ticker symbol for ISIN using OpenFIGI API"""
    payload = [{"idType": "ID_ISIN", "idValue": isin}]
    
    try:
        response = requests.post(
            OPENFIGI_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            
            if result and len(result) > 0 and 'data' in result[0]:
                figis = result[0]['data']
                
                # Look for the primary ticker (prefer Equity market sector)
                for figi in figis:
                    if 'ticker' in figi and figi.get('marketSector') == 'Equity':
                        return figi['ticker']
                
                # Fallback: return first ticker found
                for figi in figis:
                    if 'ticker' in figi:
                        return figi['ticker']
        
        return None
    
    except Exception as e:
        print(f"  ⚠ OpenFIGI error: {str(e)}")
        return None

try:
    # ========================================================================
    # STEP 1: SYNC NEW SECURITIES FROM HOLDINGS TO MAPPING TABLE
    # ========================================================================
    print("=" * 80)
    print("STEP 1: Syncing new securities from holdings")
    print("=" * 80 + "\n")
    
    # Find securities in holdings that don't exist in mapping table
    sync_query = f"""
    SELECT 
        h.security_isin,
        h.security_name
    FROM {CATALOG}.{SCHEMA}.{HOLDINGS_TABLE} h
    LEFT ANTI JOIN {CATALOG}.{SCHEMA}.{MAPPING_TABLE} m
        ON h.security_isin = m.security_isin
    ORDER BY h.security_name
    """
    
    result = execute_sql(sync_query, "Checking for new securities in holdings")
    
    new_securities = []
    data_array = result.get('result', {}).get('data_array', [])
    
    for row in data_array:
        if len(row) >= 2:
            new_securities.append({
                'isin': row[0],
                'name': row[1]
            })
    
    if new_securities:
        print(f"✓ Found {len(new_securities)} new security(ies) to sync:\n")
        for sec in new_securities:
            print(f"  - {sec['name']} ({sec['isin']})")
        print()
        
        # Insert new securities into mapping table with NULL ticker_symbol
        for sec in new_securities:
            # Escape single quotes in name
            safe_name = sec['name'].replace("'", "''")
            insert_sql = f"""
            INSERT INTO {CATALOG}.{SCHEMA}.{MAPPING_TABLE} 
            (security_isin, security_name, ticker_symbol)
            VALUES ('{sec['isin']}', '{safe_name}', NULL)
            """
            
            try:
                execute_sql(insert_sql, f"  Syncing {sec['name']}")
                print(f"    ✓ Success\n")
            except Exception as e:
                print(f"    ✗ Failed: {str(e)}\n")
        
        print(f"✓ Synced {len(new_securities)} new security(ies)\n")
    else:
        print("✓ No new securities found - mapping table is in sync\n")
    
    # ========================================================================
    # STEP 2: AUTO-POPULATE TICKER SYMBOLS VIA OPENFIGI
    # ========================================================================
    print("=" * 80)
    print("STEP 2: Auto-populating ticker symbols via OpenFIGI")
    print("=" * 80 + "\n")
    
    # Query for securities without ticker symbols
    query = f"""
    SELECT 
        security_isin,
        security_name
    FROM {CATALOG}.{SCHEMA}.{MAPPING_TABLE}
    WHERE ticker_symbol IS NULL
    ORDER BY security_name
    """
    
    result = execute_sql(query, "Fetching securities without ticker symbols")
    
    unmapped_securities = []
    data_array = result.get('result', {}).get('data_array', [])
    
    for row in data_array:
        if len(row) >= 2:
            unmapped_securities.append({
                'isin': row[0],
                'name': row[1]
            })
    
    if not unmapped_securities:
        print("✓ No securities need ticker symbols. All mapped!\n")
    else:
        print(f"✓ Found {len(unmapped_securities)} securities without ticker symbols\n")
        
        # Lookup tickers via OpenFIGI API
        updates = []
        failures = []
        
        for idx, security in enumerate(unmapped_securities, 1):
            isin = security['isin']
            name = security['name']
            
            print(f"[{idx}/{len(unmapped_securities)}] {name} ({isin})")
            
            ticker = lookup_ticker_openfigi(isin)
            
            if ticker:
                print(f"  ✓ Found ticker: {ticker}\n")
                updates.append({
                    'isin': isin,
                    'name': name,
                    'ticker': ticker
                })
            else:
                print(f"  ✗ No ticker found\n")
                failures.append({'isin': isin, 'name': name})
            
            # Rate limiting: OpenFIGI allows 25 requests/minute
            if idx % 25 == 0 and idx < len(unmapped_securities):
                print("⏳ Rate limit: waiting 60 seconds...\n")
                time.sleep(60)
            else:
                time.sleep(0.5)  # Small delay between requests
        
        # Update Databricks with found tickers
        if updates:
            print("=" * 80)
            print(f"Updating {len(updates)} ticker symbols in Databricks")
            print("=" * 80 + "\n")
            
            for update in updates:
                # Escape single quotes in ticker symbol
                safe_ticker = update['ticker'].replace("'", "''")
                update_sql = f"""
                UPDATE {CATALOG}.{SCHEMA}.{MAPPING_TABLE}
                SET ticker_symbol = '{safe_ticker}'
                WHERE security_isin = '{update['isin']}'
                """
                
                try:
                    execute_sql(update_sql, f"  Updating {update['name']} → {update['ticker']}")
                    print(f"    ✓ Success\n")
                except Exception as e:
                    print(f"    ✗ Failed: {str(e)}\n")
                    failures.append(update)
        
        # Summary for step 2
        print("=" * 80)
        print("STEP 2 SUMMARY")
        print("=" * 80)
        print(f"Unmapped securities: {len(unmapped_securities)}")
        print(f"✓ Successfully mapped: {len(updates)}")
        print(f"✗ Failed to map: {len(failures)}")
        
        if failures:
            print("\nSecurities that could not be mapped:")
            for f in failures:
                print(f"  - {f['name']} ({f['isin']})")
            print("\n⚠ These may be private companies or require manual entry.")
    
    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"✓ New securities synced: {len(new_securities)}")
    if unmapped_securities:
        print(f"✓ Ticker symbols populated: {len(updates) if 'updates' in locals() else 0}")
        print(f"⚠ Securities still unmapped: {len(failures) if 'failures' in locals() else 0}")
    else:
        print("✓ All securities have ticker symbols")
    print("=" * 80)

except Exception as e:
    print(f"\n✗ Error: {str(e)}")
    raise
