# main.py

import os
import json
import logging
import base64
from snowflake.snowpark import Session
from datetime import datetime
from dateutil import tz

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_snowflake_session():
    """
    Establishes and returns a Snowflake Snowpark Session using environment variables.
    """
    try:
        # Retrieve environment variables
        SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
        SNOWFLAKE_REGION = os.getenv('SNOWFLAKE_REGION')
        SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
        SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
        SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
        SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
        SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
        SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

        # Validate that all required environment variables are present
        required_vars = {
            'SNOWFLAKE_ACCOUNT': SNOWFLAKE_ACCOUNT,
            'SNOWFLAKE_REGION': SNOWFLAKE_REGION,
            'SNOWFLAKE_USER': SNOWFLAKE_USER,
            'SNOWFLAKE_PASSWORD': SNOWFLAKE_PASSWORD,
            'SNOWFLAKE_ROLE': SNOWFLAKE_ROLE,
            'SNOWFLAKE_WAREHOUSE': SNOWFLAKE_WAREHOUSE,
            'SNOWFLAKE_DATABASE': SNOWFLAKE_DATABASE,
            'SNOWFLAKE_SCHEMA': SNOWFLAKE_SCHEMA,
        }

        missing_vars = [key for key, value in required_vars.items() if not value]
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

        # Construct the full account identifier
        full_account = f"{SNOWFLAKE_ACCOUNT}.{SNOWFLAKE_REGION}"
        logger.info(f"Full Snowflake account identifier: {full_account}")

        # Define connection parameters
        connection_parameters = {
            "account": full_account,
            "user": SNOWFLAKE_USER,
            "password": SNOWFLAKE_PASSWORD,
            "role": SNOWFLAKE_ROLE,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA,
        }

        # Log non-sensitive connection parameters
        logger.info(f"Connection parameters: account={connection_parameters['account']}, user={connection_parameters['user']}, role={connection_parameters['role']}, warehouse={connection_parameters['warehouse']}, database={connection_parameters['database']}, schema={connection_parameters['schema']}")

        # Establish a Snowflake session
        logger.info("Establishing Snowflake session...")
        snowflake_session = Session.builder.configs(connection_parameters).create()
        logger.info("Connection to Snowflake successful!")
        return snowflake_session

    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        raise

def lambda_handler(event, context):
    # Define CORS headers
    cors_headers = {
        "Access-Control-Allow-Origin": "*",  
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization, apikey",
        "Content-Type": "application/json",
    }

    # Log the event
    logger.info(f"Event received: {json.dumps(event)}")

    # Extract the HTTP method from the event
    method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method', 'POST')

    # Handle OPTIONS method for CORS preflight
    if method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': ''
        }

    # Ensure the request method is POST
    if method != 'POST':
        return {
            'statusCode': 405,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Method Not Allowed'})
        }

    try:
        # Extract the JSON payload from the request
        body = event.get('body', '{}')
        if event.get('isBase64Encoded', False):
            body = base64.b64decode(body).decode('utf-8')
        payload = json.loads(body)
        logger.info(f"Received payload: {json.dumps(payload)}")

        # Extract 'category' from the payload
        category = payload.get('category')
        if not category:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Missing "category" in request payload.'})
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

        # Query the most recent FETCH_DATE for the given category
        try:
            sql_fetch_date = f"""
            SELECT MAX(FETCH_DATE) AS MAX_FETCH_DATE
            FROM TRADER_PORTFOLIO_AGG
            WHERE CATEGORY = '{category}'
            """
            logger.info(f"Executing query to fetch max FETCH_DATE for category '{category}'.")
            result = session.sql(sql_fetch_date).collect()
            max_fetch_date = result[0]['MAX_FETCH_DATE'] if result else None

            if not max_fetch_date:
                logger.info(f"No data found for category '{category}'.")
                session.close()
                return {
                    'statusCode': 200,
                    'headers': cors_headers,
                    'body': json.dumps({'data': []})
                }

            # Query data for the category and the most recent FETCH_DATE
            sql_query = f"""
            SELECT TOKEN, CATEGORY, TOTAL_VALUE_USD, TOTAL_BALANCE, TRADER_COUNT, FETCH_DATE
            FROM TRADER_PORTFOLIO_AGG
            WHERE CATEGORY = '{category}' AND FETCH_DATE = '{max_fetch_date}'
            """
            logger.info(f"Executing data retrieval query for category '{category}' and FETCH_DATE '{max_fetch_date}'.")
            result = session.sql(sql_query).collect()

            # Prepare data
            data = []
            for row in result:
                item = {
                    'TOKEN': row['TOKEN'],
                    'CATEGORY': row['CATEGORY'],
                    'TOTAL_VALUE_USD': row['TOTAL_VALUE_USD'],
                    'TOTAL_BALANCE': row['TOTAL_BALANCE'],
                    'TRADER_COUNT': row['TRADER_COUNT'],
                    'FETCH_DATE': row['FETCH_DATE'].isoformat() if isinstance(row['FETCH_DATE'], datetime) else row['FETCH_DATE']
                }
                data.append(item)

            logger.info(f"Retrieved {len(data)} records for category '{category}'.")

            # Close the session
            session.close()

            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({'data': data})
            }

        except Exception as e:
            logger.error(f"Error querying data: {str(e)}")
            session.close()
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Error querying data.'})
            }

    except json.JSONDecodeError:
        logger.error("Invalid JSON payload.")
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Invalid JSON payload.'})
        }
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Internal Server Error'})
        }
