from prisma import Prisma
from datetime import datetime
from typing import Optional, Dict, Any, List
import logging

prisma = Prisma()
logger = logging.getLogger(__name__)


async def connect_db():
    if not prisma.is_connected():
        await prisma.connect()
        logger.info("connected to DB")


async def disconnect_db():
    if prisma.is_connected():
        await prisma.disconnect()
        logger.info("disconnected from DB")


def create_name_id(congress: int, bill_type: str, bill_number: str) -> str:
    return f"{congress}{bill_type}{bill_number}"


def parse_date(date_string):
    if not date_string:
        logger.error("didnt get date string")
        return None
    try:
        cleaned = date_string.replace("Z", "+00,00")
        return datetime.fromisoformat(cleaned)
    except Exception as e:
        logger.error("failed parsing the data but was provided")
        print("somethign went wrong", e)
        return None


async def upsert_bill_details(bill_data):
    try:
        # required stuff
        congress = bill_data.get("congress")
        bill_type = bill_data.get("type")
        bill_number = bill_data.get("number")
        if not all([congress, bill_type, bill_number]):
            print("missing args")
            return None
        # have a bill so get the rest
        introduced_date = parse_date(bill_data.get("introducedDate"))
        bill_title = bill_data.get("title")
        bill_type = bill_data.get("type")
        bill_url = bill_data.get("url")
        #
        name_id = create_name_id(congress, bill_type, bill_number)
        legislation = await prisma.legislation.upsert(
            where={"name_id": name_id},
            data={
                "create": {
                    "name_id": name_id,
                    "congress": congress,
                    "number": bill_number,
                    "title": bill_title,
                    "type": bill_type,
                    "url": bill_url,
                },
                "update": {
                    "congress": congress,
                    "introducedDate": introduced_date,
                    "number": bill_number,
                    "title": bill_title,
                    "type": bill_type,
                    "url": bill_url,
                },
            },
        )
        action = (
            "Update" if legislation.updatedAt > legislation.createdAt else "Created"
        )
        logger.info(f"{action} bill {name_id}")
        return legislation
    except Exception as e:
        print(f"shit went wrong {e}")
        logger.error(f"fatal error in inserting data: {e}")
        return None
