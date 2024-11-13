import logging
from typing import List, Dict, Any
import os
from dotenv import load_dotenv  # Import dotenv
from utils.birdseye import BirdsEyeSDK  # Ensure this import path is correct

# Load environment variables from .env file
load_dotenv()

# Configure logging to display debug messages for testing
logging.basicConfig(level=logging.DEBUG)  # Ensure logging level is set to DEBUG
logger = logging.getLogger(__name__)

def batch_addresses(addresses: List[str], batch_size: int = 10) -> List[List[str]]:
    """
    Splits a list of addresses into smaller batches of specified size.
    """
    return [addresses[i:i + batch_size] for i in range(0, len(addresses), batch_size)]

def get_token_data_test(sdk: BirdsEyeSDK, token_addresses: List[str]) -> Dict[str, Any]:
    """
    Test function to fetch token metadata and trade data using BirdsEyeSDK.
    """
    try:
        logger.info(f"Fetching token data for {len(token_addresses)} token addresses.")
        token_data = {}
        if not token_addresses:
            logger.info("No token addresses provided for fetching token data.")
            return token_data

        # Remove duplicates and clean addresses
        unique_addresses = list(set(address.strip() for address in token_addresses if address.strip()))
        logger.info(f"Total unique token addresses to fetch: {len(unique_addresses)}")

        # Batch addresses into groups of 10
        address_batches = batch_addresses(unique_addresses, batch_size=10)
        logger.info(f"Total batches to process: {len(address_batches)}")

        for idx, batch in enumerate(address_batches, start=1):
            logger.info(f"Processing batch {idx}/{len(address_batches)}")
            try:
                # Concatenate addresses with commas
                list_address = ','.join(batch)
                logger.debug(f"List of addresses for API call: {list_address}")

                # Fetch token metadata for the current batch
                metadata_response = sdk.token.get_token_metadata_multiple(list_address)
                metadata_dict = metadata_response.get('data', {})

                # Fetch token trade data for the current batch
                trade_data_response = sdk.token.get_token_trade_data_multiple(list_address)
                trade_data_dict = trade_data_response.get('data', {})

                # Combine metadata and trade data
                for address in batch:
                    token_info = {
                        'metadata': metadata_dict.get(address, {}),
                        'trade_data': trade_data_dict.get(address, {})
                    }
                    token_data[address] = token_info

                logger.info(f"Successfully fetched data for batch {idx}")
            except Exception as e:
                logger.error(f"Error fetching data for batch {idx}: {str(e)}", exc_info=True)
                # Optionally, implement retries or continue to the next batch

        logger.info(f"Compiled token data for {len(token_data)} tokens.")
        return token_data

    except Exception as e:
        logger.error(f"Error fetching token data: {str(e)}", exc_info=True)
        return {}

# Retrieve the API key from environment variables
BIRDSEYE_API_KEY = os.getenv('BIRDSEYE_API_KEY')

# Check if API key is retrieved
if not BIRDSEYE_API_KEY:
    logger.error("BIRDSEYE_API_KEY environment variable is not set.")
    exit(1)

# Initialize the SDK with your API key
sdk = BirdsEyeSDK(api_key=BIRDSEYE_API_KEY)  # Ensure API key is correctly loaded
logger.info(f"BirdsEye SDK initialized with API key: {BIRDSEYE_API_KEY}")

# List of token addresses from your logs
list_addresses = [
    'So11111111111111111111111111111111111111112',  # Wrapped SOL
    'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'   # Marinade Staked SOL
]

# Fetch token metadata and trade data
token_data = get_token_data_test(sdk, list_addresses)

# Print the fetched token data
for address, data in token_data.items():
    print(f"Address: {address}")
    print(f"Metadata: {data['metadata']}")
    print(f"Trade Data: {data['trade_data']}")
    print("-" * 40)
