import asyncio
import logging
import os
import json
from datetime import datetime

import lib.commonutility as common
from events_db_vr2 import init_db, fetch_instana_events, process_all_events

import azure.functions as func

# Timer trigger entry point
def main(mytimer: func.TimerRequest) -> None:
    # utc_timestamp = datetime.utcnow().replace(tzinfo=None).isoformat()
    utc_timestamp = datetime.now()
    logging.info(f"Instana2BHOM function started at {utc_timestamp}")

    # Run async main logic
    try:
        asyncio.run(run_pipeline())
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")

    logging.info("Instana2BHOM function completed.")


async def run_pipeline():
    # Use same code logic as your Dockerized script
    init_db()
    await fetch_instana_events()
    process_all_events()
