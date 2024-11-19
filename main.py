# main.py

import os
import json
import logging
import base64
import decimal
from typing import List, Dict, Any
from snowflake.snowpark import Session
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Import utility functions
from utils.snowflake import (
    get_max_fetch_date,
    get_trader_portfolio_agg,
    get_addresses,
    get_top_addresses_by_frequency,
    get_trader_details,
    create_snowflake_session,
    get_token_data_from_snowflake,
    get_token_balance_changes  # Ensure this is imported if used
)
from utils.birdseye import (
    initialize_birdseye_sdk,
    get_price_data
)

# Configure logging
logger = logging.getLogger()
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    """
    AWS Lambda function handler to process API requests and return trader portfolio data,
    along with token metadata, trade data, and token balance changes. Includes proper CORS handling
    for browser-based requests.
    """
    try:
        # Log the event safely
        logger.info("Event received: {}".format(json.dumps(event)))

        # Define CORS headers
        cors_headers = {
            "Access-Control-Allow-Origin": "*",  # Update to specific origin in production
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, apikey",
            "Content-Type": "application/json",
        }

        # Extract the HTTP method from the event
        method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method', 'POST').upper()
        logger.info(f"HTTP Method: {method}")

        # Handle OPTIONS method for CORS preflight
        if method == 'OPTIONS':
            logger.info("Handling CORS preflight request.")
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': ''
            }

        # Ensure the request method is POST
        if method != 'POST':
            logger.warning(f"Method Not Allowed: {method}")
            return {
                'statusCode': 405,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Method Not Allowed'})
            }

        # Extract the JSON payload from the request
        body = event.get('body', '{}')
        is_base64_encoded = event.get('isBase64Encoded', False)
        if is_base64_encoded:
            try:
                body = base64.b64decode(body).decode('utf-8')
            except (base64.binascii.Error, UnicodeDecodeError) as e:
                logger.error(f"Error decoding base64 body: {str(e)}")
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'Invalid base64 encoding.'})
                }
        logger.info(f"Raw body: {body}")

        try:
            payload = json.loads(body)
            logger.info(f"Received payload: {json.dumps(payload)}")
        except json.JSONDecodeError:
            logger.error("Invalid JSON payload.")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Invalid JSON payload.'})
            }

        # Extract 'category' from the payload
        category = payload.get('category')
        if not category:
            logger.warning("Missing 'category' in request payload.")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Missing "category" in request payload.'})
            }

        # Extract 'addresses' from the payload (if any)
        addresses = payload.get('addresses', [])
        if addresses and not isinstance(addresses, list):
            logger.warning("'addresses' should be a list.")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': '"addresses" should be a list.'})
            }

        # Create Snowflake session
        try:
            session = create_snowflake_session()
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Failed to connect to data source.'})
            }

        # Initialize BirdsEyeSDK
        try:
            birdseye_sdk = initialize_birdseye_sdk()
        except EnvironmentError as e:
            logger.error(str(e))
            session.close()
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Server configuration error.'})
            }
        except Exception as e:
            logger.error(f"Unexpected error initializing BirdsEyeSDK: {str(e)}", exc_info=True)
            session.close()
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Server configuration error.'})
            }

        # Initialize data dictionary to hold results
        response_data = {}

        try:
            # Get the most recent FETCH_DATE for the category
            max_fetch_date = get_max_fetch_date(session, category)
            if not max_fetch_date:
                logger.info(f"No data found for category '{category}'.")
                session.close()
                return {
                    'statusCode': 200,
                    'headers': cors_headers,
                    'body': json.dumps({'data': {}})
                }

            # Define time intervals in hours and their labels
            time_intervals = [1, 2, 4, 12, 24]
            time_labels = ['1h', '2h', '4h', '12h', '24h']
            weights = {'1h': 0.4, '2h': 0.35, '4h': 0.2, '12h': 0.05, '24h': 0.0}

            # Get Data1: All rows from TRADER_PORTFOLIO_AGG for the required time intervals
            df = get_trader_portfolio_agg(session, category, max_fetch_date, time_intervals)

            # Ensure FETCH_DATE is in datetime format
            df['FETCH_DATE'] = pd.to_datetime(df['FETCH_DATE'])

            # Prepare the scoring data
            logger.info("Preparing data for scoring.")

            # -----------------------------
            # 1. Data Preparation
            # -----------------------------

            # Create a DataFrame with the most recent 'FETCH_DATE' for each token
            most_recent_dates = df.groupby('TOKEN_SYMBOL')['FETCH_DATE'].max().reset_index()
            most_recent_dates.rename(columns={'FETCH_DATE': 'MOST_RECENT_DATE'}, inplace=True)

            # Prepare the time periods DataFrame
            tokens = df['TOKEN_SYMBOL'].unique()
            tokens_df = pd.DataFrame({'TOKEN_SYMBOL': tokens})

            # Create all combinations of tokens and time periods
            tokens_periods = tokens_df.assign(key=1).merge(
                pd.DataFrame({'TIME_DELTA': [timedelta(hours=h) for h in time_intervals], 'PERIOD_LABEL': time_labels, 'key': 1}),
                on='key'
            ).drop('key', axis=1)

            # Merge most recent dates
            tokens_periods = tokens_periods.merge(most_recent_dates, on='TOKEN_SYMBOL', how='left')

            # Calculate target dates for each token and time period
            tokens_periods['TARGET_DATE'] = tokens_periods['MOST_RECENT_DATE'] - tokens_periods['TIME_DELTA']

            # Drop tokens where TARGET_DATE is before the earliest FETCH_DATE for that token
            df_min_dates = df.groupby('TOKEN_SYMBOL')['FETCH_DATE'].min().reset_index()
            df_min_dates.rename(columns={'FETCH_DATE': 'MIN_FETCH_DATE'}, inplace=True)
            tokens_periods = tokens_periods.merge(df_min_dates, on='TOKEN_SYMBOL', how='left')
            tokens_periods = tokens_periods[tokens_periods['TARGET_DATE'] >= tokens_periods['MIN_FETCH_DATE']]

            # Ensure no NaN values in merge keys
            assert tokens_periods['TOKEN_SYMBOL'].isnull().sum() == 0, "tokens_periods has NaN in 'TOKEN_SYMBOL'."
            assert tokens_periods['TARGET_DATE'].isnull().sum() == 0, "tokens_periods has NaN in 'TARGET_DATE'."
            assert df['TOKEN_SYMBOL'].isnull().sum() == 0, "df has NaN in 'TOKEN_SYMBOL'."
            assert df['FETCH_DATE'].isnull().sum() == 0, "df has NaN in 'FETCH_DATE'."

            # Sort tokens_periods and df
            tokens_periods.sort_values(['TARGET_DATE', 'TOKEN_SYMBOL'], ascending=[True, True], inplace=True)
            df.sort_values(['FETCH_DATE', 'TOKEN_SYMBOL'], ascending=[True, True], inplace=True)

            # Verify 'TARGET_DATE' is monotonically increasing within each 'TOKEN_SYMBOL'
            tokens_periods['is_increasing'] = tokens_periods.groupby('TOKEN_SYMBOL')['TARGET_DATE'].apply(lambda x: x.is_monotonic_increasing)
            assert tokens_periods['is_increasing'].all(), "TARGET_DATE is not monotonically increasing within TOKEN_SYMBOL."

            # -----------------------------
            # 2. Merge As-Of to Get Historical Data
            # -----------------------------

            # Use 'merge_asof' to find the closest historical data point for each target date
            merged = pd.merge_asof(
                tokens_periods,
                df,
                left_on='TARGET_DATE',
                right_on='FETCH_DATE',
                by='TOKEN_SYMBOL',
                direction='backward',
                suffixes=('', '_PAST')
            )

            # Rename columns from past data for clarity
            merged = merged.rename(columns={
                'FETCH_DATE': 'PAST_FETCH_DATE',
                'TOTAL_BALANCE': 'PAST_TOTAL_BALANCE',
                'TRADER_COUNT': 'PAST_TRADER_COUNT',
                # Exclude USD value as it's irrelevant
                # 'TOTAL_VALUE_USD': 'PAST_TOTAL_VALUE_USD',
                # 'CATEGORY': 'CATEGORY_PAST',
                # 'TOKEN_ADDRESS': 'TOKEN_ADDRESS_PAST'
            })

            # -----------------------------
            # 3. Retrieve Current Data for Each Token
            # -----------------------------

            # Retrieve current data for each token
            current_data = df.groupby('TOKEN_SYMBOL').last().reset_index()
            current_data = current_data.rename(columns={
                'TOTAL_BALANCE': 'CURRENT_TOTAL_BALANCE',
                'TRADER_COUNT': 'CURRENT_TRADER_COUNT',
                # 'TOTAL_VALUE_USD': 'CURRENT_TOTAL_VALUE_USD',
                'FETCH_DATE': 'CURRENT_FETCH_DATE',
                # 'CATEGORY': 'CURRENT_CATEGORY',
                # 'TOKEN_ADDRESS': 'CURRENT_TOKEN_ADDRESS'
            })

            # Merge current data into 'merged'
            merged = merged.merge(
                current_data[['TOKEN_SYMBOL', 'CURRENT_TOTAL_BALANCE', 'CURRENT_TRADER_COUNT']],
                on='TOKEN_SYMBOL',
                how='left'
            )

            # -----------------------------
            # 4. Calculate Percentage and Exponential Changes
            # -----------------------------

            # Calculate percentage change for balance
            def compute_balance_pct_change(current, past):
                if pd.isnull(past) or pd.isnull(current):
                    return 0.0  # Treat NaN changes as no change
                elif past == 0:
                    if current > 0:
                        return 100.0  # From 0 to positive value
                    else:
                        return 0.0  # No change
                else:
                    return ((current - past) / abs(past)) * 100

            merged['BALANCE_PCT_CHANGE'] = merged.apply(
                lambda row: compute_balance_pct_change(row['CURRENT_TOTAL_BALANCE'], row['PAST_TOTAL_BALANCE']),
                axis=1
            )

            # Exponentially value trader count changes
            def compute_trader_count_score(current, past):
                if pd.isnull(past) or pd.isnull(current):
                    return 0.0
                elif current <= past:
                    return 0.0  # Only value increases
                else:
                    # Exponentially value the trader count increases
                    # Formula: 2^current_trader_count - 2^past_trader_count
                    return (2 ** current) - (2 ** past)

            merged['TRADER_COUNT_SCORE'] = merged.apply(
                lambda row: compute_trader_count_score(row['CURRENT_TRADER_COUNT'], row['PAST_TRADER_COUNT']),
                axis=1
            )

            # Also calculate percentage change in trader count
            def compute_trader_count_pct_change(current, past):
                if pd.isnull(past) or pd.isnull(current):
                    return 0.0
                elif past == 0:
                    if current > 0:
                        return 100.0
                    else:
                        return 0.0
                else:
                    return ((current - past) / abs(past)) * 100

            merged['TRADER_COUNT_PCT_CHANGE'] = merged.apply(
                lambda row: compute_trader_count_pct_change(row['CURRENT_TRADER_COUNT'], row['PAST_TRADER_COUNT']),
                axis=1
            )

            # -----------------------------
            # 5. Apply Weighting Mechanism
            # -----------------------------

            # Map the weights to each period
            merged['TIME_WEIGHT'] = merged['PERIOD_LABEL'].map(weights)

            # Define metric weights
            balance_weight = 0.7
            trader_count_weight = 0.3

            # Compute the effective change
            merged['EFFECTIVE_CHANGE'] = (balance_weight * merged['BALANCE_PCT_CHANGE']) + \
                                          (trader_count_weight * merged['TRADER_COUNT_SCORE'])

            # Multiply by time weight to get the weighted change
            merged['WEIGHTED_CHANGE'] = merged['EFFECTIVE_CHANGE'] * merged['TIME_WEIGHT']

            # -----------------------------
            # 6. Compute Final Scores
            # -----------------------------

            # Sum the weighted changes for each token to compute the final score
            token_scores = merged.groupby('TOKEN_SYMBOL').agg({
                'WEIGHTED_CHANGE': 'sum',
                'CURRENT_TOTAL_BALANCE': 'first',
                'CURRENT_TRADER_COUNT': 'first'
            }).reset_index()

            # Sort tokens by their final scores in descending order
            token_scores = token_scores.sort_values('WEIGHTED_CHANGE', ascending=False).reset_index(drop=True)

            # Merge token addresses and categories back into token_scores
            token_info = df[['TOKEN_SYMBOL', 'TOKEN_ADDRESS', 'CATEGORY']].drop_duplicates()
            token_scores = token_scores.merge(token_info, on='TOKEN_SYMBOL', how='left')

            # -----------------------------
            # 7. Fetch and Merge Token Data
            # -----------------------------

            # Now, get the list of token addresses from token_scores
            token_addresses = token_scores['TOKEN_ADDRESS'].unique().tolist()
            logger.info(f"Total token addresses collected: {len(token_addresses)}")

            # Fetch token data from Snowflake
            token_data = get_token_data_from_snowflake(session, token_addresses)

            # Convert token_data to DataFrame
            token_data_df = pd.DataFrame.from_dict(token_data, orient='index')

            # Merge token_data into token_scores
            final_data = token_scores.merge(token_data_df, left_on='TOKEN_ADDRESS', right_on='TOKEN_ADDRESS', how='left')

            # -----------------------------
            # 8. Fetch Price Data and Merge
            # -----------------------------

            # Fetch price data from BirdsEyeSDK
            price_data = get_price_data(birdseye_sdk, token_addresses)

            # Convert price_data to DataFrame
            price_data_df = pd.DataFrame.from_dict(price_data, orient='index').reset_index().rename(columns={'index': 'TOKEN_ADDRESS'})

            # Merge price_data into final_data
            final_data = final_data.merge(price_data_df, on='TOKEN_ADDRESS', how='left')

            # -----------------------------
            # 9. Prepare Response Data
            # -----------------------------

            # Merge the per-interval metrics into a list for each token
            per_token_metrics = merged.groupby('TOKEN_SYMBOL').apply(lambda x: x.to_dict(orient='records')).reset_index()
            per_token_metrics.columns = ['TOKEN_SYMBOL', 'INTERVAL_METRICS']

            # Merge interval metrics into final_data
            final_data = final_data.merge(per_token_metrics, on='TOKEN_SYMBOL', how='left')

            # Convert final_data to a list of dictionaries for JSON serialization
            data3 = final_data.to_dict(orient='records')

            # Replace 'data3' with the new scoring results
            response_data['data'] = data3

            # -----------------------------
            # 10. Close the session
            # -----------------------------

            # Close the session
            session.close()

            # -----------------------------
            # 11. Ensure Data is Serializable
            # -----------------------------

            # Convert any datetime objects and Decimal objects to strings/floats
            def serialize(obj):
                if isinstance(obj, (datetime, pd.Timestamp, np.datetime64)):
                    return obj.isoformat()
                if isinstance(obj, (timedelta, pd.Timedelta)):
                    return str(obj)
                if isinstance(obj, (decimal.Decimal, np.float64, np.float32, float)):
                    return float(obj)
                if isinstance(obj, np.integer):
                    return int(obj)
                if pd.isnull(obj):
                    return None
                raise TypeError(f"Type {type(obj)} not serializable")


            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps(response_data, default=serialize)
            }

        except Exception as e:
            logger.error(f"Error querying data: {str(e)}", exc_info=True)
            session.close()
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Error querying data.'})
            }

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Internal Server Error'})
        }
