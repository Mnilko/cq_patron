import asyncio
import logging
import time
from typing import Dict, List, Tuple, Any
from pymongo import MongoClient
from dotenv import load_dotenv
import os

from async_substrate_interface import AsyncSubstrateInterface
from patrol.chain_data.runtime_groupings import group_blocks

logger = logging.getLogger(__name__)
load_dotenv()

class EventFetcher:
    def __init__(self, substrate_client):
        self.substrate_client = substrate_client
        self.semaphore = asyncio.Semaphore(1)
               # Connect to MongoDB
        mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017/")
        client = MongoClient(mongo_url)  # Update with your connection string if needed
        db = client.get_database('blockchainData')  # Update with your database name
        self.blocks_collection = db.get_collection('blocks')  # Update with your collection name
  
    async def get_current_block(self) -> int:
        current_block = await self.substrate_client.query("get_block", None)
        return current_block["header"]["number"]

    async def get_block_events(
        self,
        runtime_version: int,
        block_info: List[Tuple[int, str]],
        max_concurrent: int = 10
    ) -> Dict[int, Any]:
        """
        Fetch events for a batch of blocks for a specific runtime_version using the substrate client's query method.
        """
        # Extract block hashes for processing.
        block_hashes = [block_hash for (_, block_hash) in block_info]
        semaphore = asyncio.Semaphore(max_concurrent)

        async def preprocess_with_semaphore(block_hash):
            async with semaphore:
                # Use the query method to call the substrate's _preprocess method.
                return await self.substrate_client.query(
                    "_preprocess",
                    runtime_version,
                    None,
                    block_hash,
                    module="System",
                    storage_function="Events"
                )

        tasks = [preprocess_with_semaphore(h) for h in block_hashes]
        preprocessed_lst = await asyncio.gather(*tasks)
        errors = [r for r in preprocessed_lst if isinstance(r, Exception)]
        if errors:
            raise Exception(f"Preprocessing failed: {errors}")

        payloads = [
            AsyncSubstrateInterface.make_payload(
                str(block_hash),
                preprocessed.method,
                [preprocessed.params[0], block_hash]
            )
            for block_hash, preprocessed in zip(block_hashes, preprocessed_lst)
        ]

        responses = await asyncio.wait_for(
            self.substrate_client.query(
                "_make_rpc_request",
                runtime_version,
                payloads,
                preprocessed_lst[0].value_scale_type,
                preprocessed_lst[0].storage_item
            ),
            timeout=3
        )

        # Build a mapping from block_number to event response.
        return {
            block_number: responses[block_hash][0]
            for (block_number, block_hash) in block_info
        }
    
    async def fetch_all_events(self, block_numbers: List[int], batch_size: int = 25) -> Dict[int, Any]:
        """
        Retrieve events for all given block numbers.
        """
        start_time = time.time()

        if not block_numbers:
            logger.warning("No block numbers provided. Returning empty event dictionary.")
            return {}
        
        if any(not isinstance(b, int) for b in block_numbers):
            logger.warning("Non-integer value found in block_numbers. Returning empty event dictionary.")
            return {}

        block_numbers = set(block_numbers)

        async with self.semaphore:
            logging.info(f"Attempting to fetch event data for {len(block_numbers)} blocks...")
            # Log the range of blocks being fetched
            min_block = min(block_numbers)
            max_block = max(block_numbers)
            logging.info(f"Fetching events for blocks in range {min_block} to {max_block}")

            start_time_hashes_api = time.time()
            block_hash_tasks = [
                self.substrate_client.query("get_block_hash", None, n)
                for n in block_numbers
            ]
            block_hashes = await asyncio.gather(*block_hash_tasks)
            logging.info(f"Fetched {len(block_hashes)} block hashes from API in {time.time() - start_time_hashes_api:.2f} seconds.")
            # Get block hashes for all block numbers

            # Fetch block hashes from MongoDB
            # Query MongoDB for blocks in the range
            start_time_hashes = time.time()
            block_hash_docs = list(self.blocks_collection.find(
                {'_id': {'$gte': min_block, '$lte': max_block}},
                {'_id': 1, 'hash': 1}
            ))
            logging.info(f"Fetched {len(block_hash_docs)} block hashes from MongoDB in {time.time() - start_time_hashes:.2f} seconds.")

            # Create mapping of block numbers to hashes
            block_hash_mapping = {doc['_id']: doc['hash'] for doc in block_hash_docs}

            # Get hashes for requested blocks, or fetch them if missing
            block_hashes = []
            block_numbers_to_fetch = []
            block_info = []

            for block_num in block_numbers:
                if block_num in block_hash_mapping:
                    hash_value = block_hash_mapping[block_num]
                    block_hashes.append(hash_value)
                    block_info.append((block_num, hash_value))
                else:
                    block_numbers_to_fetch.append(block_num)

            # Fetch any missing block hashes from the chain
            if block_numbers_to_fetch:
                logger.info(f"Fetching {len(block_numbers_to_fetch)} missing block hashes from the chain")
                block_hash_tasks = [
                    self.substrate_client.query("get_block_hash", None, n)
                    for n in block_numbers_to_fetch
                ]
                missing_hashes = await asyncio.gather(*block_hash_tasks)
                
                for block_num, hash_value in zip(block_numbers_to_fetch, missing_hashes):
                    block_hashes.append(hash_value)
                    block_info.append((block_num, hash_value))

            current_block = await self.get_current_block()

            versions = self.substrate_client.return_runtime_versions()
            grouped = group_blocks(block_numbers, block_hashes, current_block, versions, batch_size)

            all_events: Dict[int, Any] = {}
            start_time_fetchEvents = time.time()
            for runtime_version, batches in grouped.items():
                for batch in batches:
                    logger.info(f"Fetching events for runtime version {runtime_version} (batch of {len(batch)} blocks)...")
                    try:
                        events = await self.get_block_events(runtime_version, batch)
                        logger.info(f"Successfully fetched events for runtime version {runtime_version}. Updating...")
                        all_events.update(events)
                        logger.info(f"Updated events for runtime version {runtime_version}.")
                    except Exception as e:
                        logger.warning(
                            f"Unable to fetch events for runtime version {runtime_version} batch on final attempt: {e}. Continuing..."
                        )
            logger.info(f"Fetched events for all blocks in {time.time() - start_time_fetchEvents:.2f} seconds.")
        # Continue to next version even if the current one fails.
        logger.info(f"All events collected in {time.time() - start_time} seconds.")
        return all_events

async def example():

    import json

    from patrol.chain_data.substrate_client import SubstrateClient
    from patrol.chain_data.runtime_groupings import load_versions

    network_url = "wss://archive.chain.opentensor.ai:443/"
    versions = load_versions()
    
    client = SubstrateClient(runtime_mappings=versions, network_url=network_url, max_retries=3)
    await client.initialize()

    fetcher = EventFetcher(substrate_client=client)

    test_cases = [
        [5163655 + i for i in range(1000)],
        # [3804341 + i for i in range(1000)]    # high volume
    ]

    for test_case in test_cases:

        logger.info("Starting next test case.")

        start_time = time.time()
        all_events = await fetcher.fetch_all_events(test_case, 50)
        logger.info(f"\nRetrieved events for {len(all_events)} blocks in {time.time() - start_time:.2f} seconds.")

        with open('raw_event_data.json', 'w') as file:
            json.dump(all_events, file, indent=4)

        # bt.logging.debug(all_events)


if __name__ == "__main__":
    asyncio.run(example())
