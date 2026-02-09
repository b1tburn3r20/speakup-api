import requests
import os
import asyncio
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from variables import VALID_BILL_TYPES
from insert import connect_db, disconnect_db, upsert_bill_details
import logging


load_dotenv()

CONGRESS_API_KEY = os.getenv("CONGRESS_API_KEY")
CONGRESS_NUMBER = 119


def setup_logger():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file = os.path.join(
        log_dir, f"congress_import_{datetime.now().strftime('%Y%m%d')}.log"
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )


logger = logging.getLogger(__name__)


async def fetchLatestBills():
    url = f"https://api.congress.gov/v3/bill/{119}"
    params = {"api_key": CONGRESS_API_KEY, "format": "json"}
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            res = response.json()
            await processLatestBillsData(res)
        else:
            print("Something went wrong", response.status_code)
            logger.error(f"Failed to fetch bills: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print("Real error", e)
        logger.error(f"request error {e}")
        return None


async def processLatestBillsData(data):
    bill_data = data["bills"]
    logger.info(f"processing {len(bill_data)} bills")
    success_count = 0
    fail_count = 0
    for index, bill in enumerate(bill_data, 1):
        result = await fetchBillDetails(bill)
        if result:
            success_count += 1
        else:
            fail_count += 1
    logger.info(f"Completed: {success_count} successful, {fail_count} failed")


async def fetchBillSummaries(bill):
    bill_congress = bill["congress"]
    bill_type = bill["type"]
    bill_number = bill["number"]
    if bill_type not in VALID_BILL_TYPES:
        return
    url = f"https://api.congress.gov/v3/bill/{bill_congress}/{bill_type}/{bill_number}/summaries"
    params = {"api_key": CONGRESS_API_KEY, "format": "json"}
    response = await asyncio.to_thread(requests.get, url, params=params)
    if response.status_code == 200:
        res = response.json()
        dumped = json.dumps(res, indent=4)
        print(dumped)
    else:
        print("Couldnt get a summary: ", response.status_code, response.text)

    time.sleep(1)


async def fetchBillRelatedBills(bill):
    bill_congress = bill["congress"]
    bill_type = bill["type"]
    bill_number = bill["number"]
    if bill_type not in VALID_BILL_TYPES:
        return
    url = f"https://api.congress.gov/v3/bill/{bill_congress}/{bill_type}/{bill_number}/relatedbills"
    params = {"api_key": CONGRESS_API_KEY, "format": "json"}
    response = await asyncio.to_thread(requests.get, url, params=params)
    if response.status_code == 200:
        res = response.json()
        dumped = json.dumps(res, indent=4)
        print(dumped)
    else:
        print("couldnt fetch the relatedbills", response.status_code, response.text)
    time.sleep(1)


async def fetchBillCosponsors(bill):
    bill_congress = bill["congress"]
    bill_type = bill["type"]
    bill_number = bill["number"]
    if bill_type not in VALID_BILL_TYPES:
        return
    url = f"https://api.congress.gov/v3/bill/{bill_congress}/{bill_type}/{bill_number}/cosponsors"
    params = {"api_key": CONGRESS_API_KEY, "format": "json"}
    response = await asyncio.to_thread(requests.get, url, params=params)
    if response.status_code == 200:
        res = response.json()
        dumped = json.dumps(res, indent=4)
        print(dumped)
    else:
        print("something went wrong", response.status_code, response.text)
    time.sleep(1)


async def fetchBillActions(bill):
    bill_congress = bill["congress"]
    bill_type = bill["type"]
    bill_number = bill["number"]
    if bill_type not in VALID_BILL_TYPES:
        return
    url = f"https://api.congress.com/v3/bill/{bill_congress}/{bill_type}/{bill_number}/actions"
    params = {"api_key": CONGRESS_API_KEY, "format": "json"}
    response = await asyncio.to_thread(requests.get, url, params=params)
    if response.status_code == 200:
        res = response.json()
        dumped = json.dumps(res)
        print(dumped)
    else:
        print("coulndt fetch the action data", response.status_code, response.text)
    time.sleep(1)


async def fetchBillDetails(bill):
    bill_congress = bill["congress"]
    bill_type = bill["type"]
    bill_number = bill["number"]
    if bill_type not in VALID_BILL_TYPES:
        return
    url = f"https://api.congress.gov/v3/bill/{bill_congress}/{bill_type}/{bill_number}"
    params = params = {"api_key": CONGRESS_API_KEY}
    response = await asyncio.to_thread(requests.get, url, params=params)
    if response.status_code == 200:
        res = response.json()
        bill_data = res.get("bill")
        if bill_data:
            legislation = await upsert_bill_details(bill_data)
            return legislation
        else:
            print("no bill data for this bill", res)
            logger.warning("No bill data in response")
        # dumped = json.dumps(res, indent=4)
        # print(dumped)
    else:
        print("something went wrong", response)
        logger.warning("No bill data in response")
        return None

    time.sleep(1)


async def main():
    logger.info("connecting to db")
    await connect_db()
    logger.info("connected")
    try:
        await fetchLatestBills()
        logger.info("bills got")
    except Exception as e:
        logger.error("error", e)
    finally:
        logger.info("disconnecting")
        await disconnect_db()
        logger.info("disconnected")


if __name__ == "__main__":
    setup_logger()
    asyncio.run(main())
