# main.py

import os
import json
import logging
import base64
from snowflake.snowpark import Session

# Import utility functions
from utils.snowflake import (
    get_max_fetch_date,
    get_trader_portfolio_agg,
    get_addresses,
    get_top_addresses_by_frequency,
    get_trader_details,
    create_snowflake_session
)
from utils.birdseye import (
    initialize_birdseye_sdk,
    get_token_data
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
    along with token metadata and trade data. Includes proper CORS handling for browser-based requests.
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
            body = base64.b64decode(body).decode('utf-8')
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
        # Renamed from 'address' to 'addresses' for clarity
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

            # Get Data1: All rows from TRADER_PORTFOLIO_AGG for the most recent date and category
            data1 = get_trader_portfolio_agg(session, category, max_fetch_date)
            response_data['data1'] = data1

            # Get Data2: List of trader addresses from TRADERS table for the category
            data2 = get_addresses(session, category)
            response_data['data2'] = data2

            # Get Data3: Trader details for a set of addresses
            # If 'addresses' provided in payload, use them. Else, get top 5 addresses based on frequency from TRADERS
            if not addresses:
                addresses = get_top_addresses_by_frequency(session, category, top_n=5)
            data3 = get_trader_details(session, addresses)
            response_data['data3'] = data3

            # Now, get the list of token addresses from data1
            token_addresses = set()

            # From data1
            for item in data1:
                if 'TOKEN_ADDRESS' in item and item['TOKEN_ADDRESS']:
                    token_addresses.add(item['TOKEN_ADDRESS'])

            # If token addresses are available from other sources, add them here
            # Currently, data3 does not provide 'TOKEN_ADDRESSES', so we skip it

            token_addresses = list(token_addresses)
            logger.info(f"Total token addresses collected: {len(token_addresses)}")

            # Use the SDK to fetch token metadata and trade data
            token_data = get_token_data(birdseye_sdk, token_addresses)
            response_data['token_data'] = token_data

            # Close the session
            session.close()

            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({'data': response_data})
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
