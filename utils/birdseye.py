# utils/birdseye.py

import os
import logging
from typing import List, Dict, Any
from birdseye_sdk import BirdsEyeSDK

# Use the root logger
logger = logging.getLogger()

def initialize_birdseye_sdk() -> BirdsEyeSDK:
    """
    Initializes and returns a BirdsEyeSDK instance using the API key from environment variables.
    """
    BIRDSEYE_API_KEY = os.getenv('BIRDSEYE_API_KEY')
    if not BIRDSEYE_API_KEY:
        logger.error("BIRDSEYE_API_KEY not set in environment variables.")
        raise EnvironmentError("BIRDSEYE_API_KEY not set in environment variables.")

    logger.info("Initializing BirdsEyeSDK.")
    try:
        birdseye_sdk = BirdsEyeSDK(api_key=BIRDSEYE_API_KEY)
        logger.info("BirdsEyeSDK initialized successfully.")
        return birdseye_sdk
    except Exception as e:
        logger.error(f"Failed to initialize BirdsEyeSDK: {str(e)}", exc_info=True)
        raise

def batch_addresses(addresses: List[str], batch_size: int = 0) -> List[List[str]]:
    """
    Splits a list of addresses into smaller batches of specified size.

    :param addresses: List of token addresses.
    :param batch_size: Maximum number of addresses per batch.
    :return: List of address batches.
    """
    return [addresses[i:i + batch_size] for i in range(0, len(addresses), batch_size)]

def get_token_data(sdk: BirdsEyeSDK, token_addresses: List[str]) -> dict:
    """
    Fetches token metadata and price/volume data for a list of token addresses using the BirdsEyeSDK.
    Returns a dictionary mapping token addresses to their respective data.
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
        address_batches = batch_addresses(unique_addresses, batch_size=100)
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

                # Fetch price and volume data for the current batch
                price_volume_response = sdk.defi.get_price_volume_multi(list_address, type="24h")
                price_volume_dict = price_volume_response.get('data', {})

                # Combine metadata and price/volume data
                for address in batch:
                    token_info = {
                        'metadata': metadata_dict.get(address, {}),
                        'price_volume_data': price_volume_dict.get(address, {})
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
