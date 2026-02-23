import requests
import os
import asyncio
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from variables import VALID_BILL_TYPES
from insert import prisma

import logging
from insert import (
    connect_db,
    disconnect_db,
    upsert_bill_details,
    insert_bill_actions,
    insert_bill_summaries,
    insert_house_vote,
    insert_member_votes,
)

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
    for index, bill in enumerate(bill_data, 1):
        result = await fetchBillActions(bill)
        if result:
            success_count += 1
        else:
            fail_count += 1
    logger.info(f"Completed: {success_count} successful, {fail_count} failed")

    logger.info(
        """
=============================================================================================================================================

    STARTING SUMMARIES 

=============================================================================================================================================
           """
    )

    for index, bill in enumerate(bill_data, 1):
        result = await fetchBillSummaries(bill)
        if result:
            success_count += 1
        else:
            fail_count += 1
    logger.info(f"Completed: {success_count} successful, {fail_count} failed")


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


async def fetchBillSummaries(bill):
    bill_congress = bill["congress"]
    bill_type = bill["type"]
    bill_number = bill["number"]
    if bill_type not in VALID_BILL_TYPES:
        return
    url = f"https://api.congress.gov/v3/bill/{bill_congress}/{bill_type.lower()}/{bill_number}/summaries"
    params = {"api_key": CONGRESS_API_KEY, "format": "json"}
    response = await asyncio.to_thread(requests.get, url, params=params)
    if response.status_code == 200:
        res = response.json()
        if res:
            legislation = await insert_bill_summaries(bill, res)
            return legislation
    else:
        print("couldnt fetch the summary data", response.status_code, response.text)
    time.sleep(1)


async def fetchBillActions(bill):
    bill_congress = bill["congress"]
    bill_type = bill["type"]
    bill_number = bill["number"]
    if bill_type not in VALID_BILL_TYPES:
        return
    url = f"https://api.congress.gov/v3/bill/{bill_congress}/{bill_type.lower()}/{bill_number}/actions"
    params = {"api_key": CONGRESS_API_KEY, "format": "json"}
    response = await asyncio.to_thread(requests.get, url, params=params)
    if response.status_code == 200:
        res = response.json()
        if res:
            legislation = await insert_bill_actions(bill, res)
            return legislation
    else:
        print("couldnt fetch the action data", response.status_code, response.text)
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


async def fetchHouseVotes():
    url = f"https://api.congress.gov/v3/house-vote/{CONGRESS_NUMBER}"
    params = {"api_key": CONGRESS_API_KEY, "format": "json", "limit": 250, "offset": 0}

    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            res = response.json()
            dumped = json.dumps(res, indent=2)
            lines = dumped.split("\n")
            print("\n".join(lines[:200]))
            await processHouseVotes(res)
        else:
            print("Something went wrong fetching house votes", response.status_code)
            logger.error(f"Failed to fetch house votes: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print("Real error", e)
        logger.error(f"request error {e}")
        return None


async def processHouseVotes(data):
    votes_data = data.get("houseRollCallVotes", [])
    total_votes = len(votes_data)
    logger.info(f"processing {total_votes} house votes")

    # Fetch ALL congress members once at the start using the imported function
    logger.info("Loading all congress members into cache...")
    from insert import prisma  # Import prisma from insert module

    all_members = await prisma.congressmember.find_many()
    member_cache = {cm.bioguideId: cm for cm in all_members}

    success_count = 0
    fail_count = 0

    for index, vote in enumerate(votes_data, 1):
        # Calculate progress percentage
        progress_pct = (index / total_votes) * 100

        result = await insert_house_vote(vote)
        if result:
            success_count += 1
            # Fetch member votes for this vote - pass the cache
            congress = vote.get("congress")
            session = vote.get("sessionNumber")
            roll_number = vote.get("rollCallNumber")
            if congress and session and roll_number:
                await fetchHouseVoteMembers(
                    result, congress, session, roll_number, member_cache
                )
        else:
            fail_count += 1

        # Log progress every 10 votes or on the last vote
        if index % 10 == 0 or index == total_votes:
            logger.info(
                f"""
=============================================================================================================================================

                    Progress: {index}/{total_votes} votes ({progress_pct:.1f}%) - {success_count} successful, {fail_count} failed
           
=============================================================================================================================================
           """
            )

    logger.info(
        f"House votes completed: {success_count} successful, {fail_count} failed out of {total_votes} total"
    )


async def fetchHouseVoteMembers(vote_obj, congress, session, roll_number, member_cache):
    url = f"https://api.congress.gov/v3/house-vote/{congress}/{session}/{roll_number}/members"
    params = {"api_key": CONGRESS_API_KEY, "format": "json"}
    response = await asyncio.to_thread(requests.get, url, params=params)
    if response.status_code == 200:
        res = response.json()
        if res:
            # Pass the member_cache to insert_member_votes
            result = await insert_member_votes(vote_obj.id, res, member_cache)
            return result
    else:
        print(f"couldnt fetch member votes: {response.status_code}")
    time.sleep(1)


async def main():
    logger.info("connecting to db")
    await connect_db()
    logger.info("connected")
    try:
        await fetchLatestBills()
        logger.info("bills processing complete")
        await fetchHouseVotes()
        logger.info("house votes processing complete")
    except Exception as e:
        logger.error(f"error: {e}")
    finally:
        logger.info("disconnecting")
        await disconnect_db()
        logger.info("disconnected")


if __name__ == "__main__":
    setup_logger()
    asyncio.run(main())
