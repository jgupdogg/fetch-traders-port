# utils/snowflake.py

import os
import logging
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from typing import List, Dict, Any

# Use the root logger
logger = logging.getLogger()

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
        logger.error(f"Failed to connect to Snowflake: {str(e)}", exc_info=True)
        raise

def get_max_fetch_date(session: Session, category: str) -> datetime:
    """
    Retrieves the most recent FETCH_DATE for the given category.
    """
    try:
        sql_fetch_date = f"""
        SELECT MAX(FETCH_DATE) AS MAX_FETCH_DATE
        FROM TRADER_PORTFOLIO_AGG
        WHERE CATEGORY = '{category}'
        """
        logger.info(f"Executing query to fetch max FETCH_DATE for category '{category}'.")
        result = session.sql(sql_fetch_date).collect()
        max_fetch_date = result[0]['MAX_FETCH_DATE'] if result else None
        logger.info(f"Max FETCH_DATE for category '{category}': {max_fetch_date}")
        return max_fetch_date
    except Exception as e:
        logger.error(f"Error fetching max FETCH_DATE: {str(e)}", exc_info=True)
        raise

def get_trader_portfolio_agg(session: Session, category: str, fetch_date: datetime) -> list:
    """
    Retrieves all rows from TRADER_PORTFOLIO_AGG for the specified category and fetch_date.
    """
    try:
        sql_query = f"""
        SELECT TOKEN_SYMBOL AS TOKEN, TOKEN_ADDRESS, CATEGORY, TOTAL_VALUE_USD, TOTAL_BALANCE, TRADER_COUNT, FETCH_DATE
        FROM TRADER_PORTFOLIO_AGG
        WHERE CATEGORY = '{category}' AND FETCH_DATE = '{fetch_date}'
        """
        logger.info(f"Executing data retrieval query for category '{category}' and FETCH_DATE '{fetch_date}'.")
        result = session.sql(sql_query).collect()

        # Prepare data
        data = []
        for row in result:
            item = {
                'TOKEN': row['TOKEN'],
                'TOKEN_ADDRESS': row['TOKEN_ADDRESS'],
                'CATEGORY': row['CATEGORY'],
                'TOTAL_VALUE_USD': row['TOTAL_VALUE_USD'],
                'TOTAL_BALANCE': row['TOTAL_BALANCE'],
                'TRADER_COUNT': row['TRADER_COUNT'],
                'FETCH_DATE': row['FETCH_DATE'].isoformat() if isinstance(row['FETCH_DATE'], datetime) else row['FETCH_DATE']
            }
            data.append(item)
        logger.info(f"Retrieved {len(data)} records for category '{category}'.")
        return data
    except Exception as e:
        logger.error(f"Error fetching trader portfolio aggregation: {str(e)}", exc_info=True)
        raise

def get_addresses(session: Session, category: str) -> list:
    """
    Retrieves all trader addresses from the TRADERS table for the specified category.
    """
    try:
        sql_query = f"""
        SELECT ADDRESS
        FROM TRADERS
        WHERE CATEGORY = '{category}'
        """
        logger.info(f"Fetching addresses from TRADERS table for category '{category}'.")
        result = session.sql(sql_query).collect()

        addresses = [row['ADDRESS'] for row in result]
        logger.info(f"Retrieved {len(addresses)} addresses for category '{category}'.")
        return addresses
    except Exception as e:
        logger.error(f"Error fetching addresses: {str(e)}", exc_info=True)
        raise

def get_top_addresses_by_frequency(session: Session, category: str, top_n: int = 5) -> list:
    """
    Retrieves the top N addresses by frequency (FREQ) from the TRADERS table for the specified category.
    """
    try:
        sql_query = f"""
        SELECT ADDRESS
        FROM TRADERS
        WHERE CATEGORY = '{category}'
        ORDER BY FREQ DESC
        LIMIT {top_n}
        """
        logger.info(f"Fetching top {top_n} addresses by frequency for category '{category}'.")
        result = session.sql(sql_query).collect()

        addresses = [row['ADDRESS'] for row in result]
        logger.info(f"Retrieved top {len(addresses)} addresses by frequency.")
        return addresses
    except Exception as e:
        logger.error(f"Error fetching top addresses by frequency: {str(e)}", exc_info=True)
        raise

def get_trader_details(session: Session, addresses: list) -> list:
    """
    Retrieves trader details for the given list of addresses from the TRADERS table.
    """
    try:
        if not addresses:
            logger.info("No addresses provided for fetching trader details.")
            return []

        logger.info(f"Fetching trader details for addresses: {addresses}")

        # Use Snowpark's DataFrame API for safer and more efficient queries
        df = session.table('TRADERS') \
                     .filter(col('ADDRESS').isin(addresses)) \
                     .select('DATE_ADDED', 'ADDRESS', 'CATEGORY', 'FREQ') \
                     .order_by('ADDRESS')

        result = df.collect()

        # Prepare data
        data = []
        for row in result:
            item = {
                'DATE_ADDED': row['DATE_ADDED'].isoformat() if isinstance(row['DATE_ADDED'], datetime) else row['DATE_ADDED'],
                'ADDRESS': row['ADDRESS'],
                'CATEGORY': row['CATEGORY'],
                'FREQ': row['FREQ']
            }
            data.append(item)
        logger.info(f"Retrieved {len(data)} trader records.")
        return data
    except Exception as e:
        logger.error(f"Error fetching trader details: {str(e)}", exc_info=True)
        raise


def get_token_data_from_snowflake(session: Session, token_addresses: List[str]) -> Dict[str, Any]:
    """
    Fetches token data from the TOKEN_DATA table for the given token addresses.
    Returns a dictionary mapping token addresses to their respective data.
    """
    try:
        if not token_addresses:
            logger.info("No token addresses provided for fetching token data.")
            return {}
        logger.info(f"Fetching token data for addresses: {token_addresses}")

        # Use Snowpark's DataFrame API to prevent SQL injection
        df = session.table('TOKEN_DATA').filter(col('TOKEN_ADDRESS').isin(token_addresses))
        result = df.collect()

        # Prepare data
        token_data = {}
        for row in result:
            token_address = row['TOKEN_ADDRESS']
            item = {
                'TOKEN_ADDRESS': token_address,
                'SYMBOL': row['SYMBOL'],
                'DECIMALS': row['DECIMALS'],
                'NAME': row['NAME'],
                'WEBSITE': row['WEBSITE'],
                'TWITTER': row['TWITTER'],
                'DESCRIPTION': row['DESCRIPTION'],
                'LOGO_URI': row['LOGO_URI'],
                'LIQUIDITY': row['LIQUIDITY'],
                'MARKET_CAP': row['MARKET_CAP'],
                'HOLDER_COUNT': row['HOLDER_COUNT'],
                'PRICE': row['PRICE'],
                'V24H_USD': row['V24H_USD'],
                'V_BUY_HISTORY_24H_USD': row['V_BUY_HISTORY_24H_USD'],
                'V_SELL_HISTORY_24H_USD': row['V_SELL_HISTORY_24H_USD'],
                'CREATION_TIMESTAMP': row['CREATION_TIMESTAMP'].isoformat() if isinstance(row['CREATION_TIMESTAMP'], datetime) else row['CREATION_TIMESTAMP'],
                'OWNER': row['OWNER'],
                'TOP10_HOLDER_PERCENT': row['TOP10_HOLDER_PERCENT'],
                'OWNER_PERCENTAGE': row['OWNER_PERCENTAGE'],
                'CREATOR_PERCENTAGE': row['CREATOR_PERCENTAGE'],
                'LAST_UPDATED': row['LAST_UPDATED'].isoformat() if isinstance(row['LAST_UPDATED'], datetime) else row['LAST_UPDATED'],
                'DATE_ADDED': row['DATE_ADDED'].isoformat() if isinstance(row['DATE_ADDED'], datetime) else row['DATE_ADDED'],
            }
            token_data[token_address] = item

        logger.info(f"Retrieved token data for {len(token_data)} tokens.")
        return token_data
    except Exception as e:
        logger.error(f"Error fetching token data from Snowflake: {str(e)}", exc_info=True)
        raise