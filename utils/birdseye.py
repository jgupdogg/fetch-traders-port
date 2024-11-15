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

def get_price_data(sdk: BirdsEyeSDK, token_addresses: List[str]) -> dict:
    """
    Fetches price data for a list of token addresses using the BirdsEyeSDK.
    Returns a dictionary mapping token addresses to their respective price data.
    """
    try:
        logger.info(f"Fetching price data for {len(token_addresses)} token addresses.")
        price_data = {}
        if not token_addresses:
            logger.info("No token addresses provided for fetching price data.")
            return price_data

        # Remove duplicates and clean addresses
        unique_addresses = list(set(address.strip() for address in token_addresses if address.strip()))
        logger.info(f"Total unique token addresses to fetch: {len(unique_addresses)}")

        # Batch addresses into groups of 100
        address_batches = batch_addresses(unique_addresses, batch_size=100)
        logger.info(f"Total batches to process: {len(address_batches)}")

        for idx, batch in enumerate(address_batches, start=1):
            logger.info(f"Processing batch {idx}/{len(address_batches)}")
            try:
                # Concatenate addresses with commas
                list_address = ','.join(batch)
                logger.debug(f"List of addresses for API call: {list_address}")

                # Fetch price and volume data for the current batch
                price_volume_response = sdk.defi.get_price_volume_multi(list_address, type="24h")
                price_volume_dict = price_volume_response.get('data', {})

                # Add price data to the result
                for address in batch:
                    price_info = price_volume_dict.get(address, {})
                    price_data[address] = price_info

                logger.info(f"Successfully fetched price data for batch {idx}")
            except Exception as e:
                logger.error(f"Error fetching price data for batch {idx}: {str(e)}", exc_info=True)
                # Optionally, implement retries or continue to the next batch

        logger.info(f"Compiled price data for {len(price_data)} tokens.")
        return price_data

    except Exception as e:
        logger.error(f"Error fetching price data: {str(e)}", exc_info=True)
        return {}
