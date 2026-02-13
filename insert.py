from prisma import Prisma
from datetime import datetime
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
        cleaned = date_string.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except Exception as e:
        logger.error(f"failed parsing the date: {e}")
        print("something went wrong", e)
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


async def insert_bill_actions(bill_data, actions_data):
    try:
        # required stuff
        congress = bill_data.get("congress")
        bill_type = bill_data.get("type")
        bill_number = bill_data.get("number")
        if not all([congress, bill_type, bill_number]):
            print("missing args")
            return None

        # Get the legislation record
        name_id = create_name_id(congress, bill_type, bill_number)
        legislation = await prisma.legislation.find_unique(where={"name_id": name_id})

        if not legislation:
            print(f"legislation {name_id} not found")
            logger.error(f"Legislation {name_id} not found")
            return None

        # Get actions array
        actions = actions_data.get("actions", [])
        if not actions:
            print("no actions in response")
            logger.warning("No actions in response")
            return legislation

        success_count = 0
        fail_count = 0

        for action in actions:
            action_date_str = action.get("actionDate")
            action_text = action.get("text")
            action_type = action.get("type")
            action_code = action.get("actionCode")

            if not all([action_date_str, action_text, action_type]):
                fail_count += 1
                continue

            action_date = parse_date(action_date_str)
            if not action_date:
                fail_count += 1
                continue

            # Check if exists
            existing = await prisma.billaction.find_first(
                where={
                    "legislationId": legislation.id,
                    "actionDate": action_date,
                    "text": action_text,
                    "type": action_type,
                }
            )

            if existing:
                success_count += 1
                continue

            # Insert action
            await prisma.billaction.create(
                data={
                    "legislationId": legislation.id,
                    "actionDate": action_date,
                    "text": action_text,
                    "type": action_type,
                    "actionCode": action_code,
                }
            )
            success_count += 1

        logger.info(
            f"Bill {name_id}: {success_count} actions processed, {fail_count} failed"
        )
        return legislation
    except Exception as e:
        print(f"shit went wrong {e}")
        logger.error(f"fatal error in inserting actions: {e}")
        return None


async def insert_bill_summaries(bill_data, summaries_data):
    try:
        # required stuff
        congress = bill_data.get("congress")
        bill_type = bill_data.get("type")
        bill_number = bill_data.get("number")
        if not all([congress, bill_type, bill_number]):
            print("missing args")
            return None

        name_id = create_name_id(congress, bill_type, bill_number)

        # Get the legislation record
        legislation = await prisma.legislation.find_unique(where={"name_id": name_id})

        if not legislation:
            print(f"legislation {name_id} not found")
            logger.error(f"Legislation {name_id} not found")
            return None

        # Get summaries array
        summaries = summaries_data.get("summaries", [])
        if not summaries:
            print("no summaries in response")
            logger.warning("No summaries in response")
            return legislation

        success_count = 0
        fail_count = 0

        for summary in summaries:
            action_date_str = summary.get("actionDate")
            action_desc = summary.get("actionDesc")
            text = summary.get("text")
            update_date_str = summary.get("updateDate")
            version_code = summary.get("versionCode")

            if not text:
                fail_count += 1
                continue

            action_date = parse_date(action_date_str) if action_date_str else None
            update_date = parse_date(update_date_str) if update_date_str else None

            # Check if exists
            existing = await prisma.billsummary.find_first(
                where={
                    "legislationId": legislation.id,
                    "versionCode": version_code,
                }
            )

            if existing:
                # Update existing summary
                await prisma.billsummary.update(
                    where={"id": existing.id},
                    data={
                        "actionDate": action_date,
                        "actionDesc": action_desc,
                        "text": text,
                        "updateDate": update_date,
                    },
                )
                success_count += 1
                continue

            # Insert summary
            await prisma.billsummary.create(
                data={
                    "legislationId": legislation.id,
                    "actionDate": action_date,
                    "actionDesc": action_desc,
                    "text": text,
                    "updateDate": update_date,
                    "versionCode": version_code,
                }
            )
            success_count += 1

        logger.info(
            f"Bill {name_id}: {success_count} summaries processed, {fail_count} failed"
        )
        return legislation
    except Exception as e:
        print(f"shit went wrong {e}")
        logger.error(f"fatal error in inserting summaries: {e}")
        return None


async def insert_house_vote(vote_data):
    try:
        # required stuff
        congress = vote_data.get("congress")
        roll_call_number = vote_data.get("rollCallNumber")
        session_number = vote_data.get("sessionNumber")

        if not all([congress, roll_call_number]):
            print("missing args for house vote")
            return None

        # get the rest
        legislation_number = vote_data.get("legislationNumber")
        legislation_type = vote_data.get("legislationType")
        result = vote_data.get("result")
        start_date = parse_date(vote_data.get("startDate"))
        vote_type = vote_data.get("voteType")
        vote_question = vote_data.get("voteQuestion")

        # Create name_id for linking to legislation
        name_id = None
        if legislation_number and legislation_type:
            name_id = create_name_id(congress, legislation_type, legislation_number)

        # Find existing vote using find_first
        existing_vote = await prisma.vote.find_first(
            where={
                "congress": congress,
                "chamber": "HOUSE",  # FIXED: Use correct enum value
                "rollNumber": roll_call_number,
            }
        )

        if existing_vote:
            # Update existing vote
            vote = await prisma.vote.update(
                where={"id": existing_vote.id},
                data={
                    "date": start_date if start_date else datetime.now(),
                    "description": vote_question,
                    "question": vote_question,
                    "result": result,
                    "billNumber": legislation_number,
                    "name_id": name_id,
                },
            )
            logger.info(f"Updated house vote {congress}/{roll_call_number}")
        else:
            # Create new vote
            vote = await prisma.vote.create(
                data={
                    "congress": congress,
                    "chamber": "HOUSE",  # FIXED: Use correct enum value
                    "rollNumber": roll_call_number,
                    "date": start_date if start_date else datetime.now(),
                    "description": vote_question,
                    "question": vote_question,
                    "result": result,
                    "billNumber": legislation_number,
                    "name_id": name_id,
                    "totalYea": 0,
                    "totalNay": 0,
                    "totalNotVoting": 0,
                    "totalPresent": 0,
                }
            )
            logger.info(f"Created house vote {congress}/{roll_call_number}")

        return vote
    except Exception as e:
        print(f"shit went wrong {e}")
        logger.error(f"fatal error in inserting house vote: {e}")
        return None


async def insert_member_votes(vote_id, members_data, member_cache=None):
    """
    Optimized version with member caching and batch operations.

    Args:
        vote_id: The vote ID
        members_data: API response data
        member_cache: Dict mapping bioguideId -> member object (optional)
    """
    try:
        # Get the correct nested structure
        vote_data = members_data.get("houseRollCallVoteMemberVotes", {})
        members = vote_data.get("results", [])

        if not members:
            print("no members in response")
            logger.warning("No members in response")
            return None

        # If no cache provided, fetch all members at once
        if member_cache is None:
            all_bioguide_ids = [
                m.get("bioguideID") for m in members if m.get("bioguideID")
            ]
            congress_members = await prisma.congressmember.find_many(
                where={"bioguideId": {"in": all_bioguide_ids}}
            )
            member_cache = {cm.bioguideId: cm for cm in congress_members}

        # Get existing member votes for this vote in one query
        existing_votes = await prisma.membervote.find_many(where={"voteId": vote_id})
        existing_member_ids = {mv.memberId for mv in existing_votes}

        success_count = 0
        fail_count = 0

        # Track vote totals
        total_yea = 0
        total_nay = 0
        total_present = 0
        total_not_voting = 0

        # Batch insert data
        votes_to_insert = []

        for member in members:
            bioguide_id = member.get("bioguideID")
            vote_cast = member.get("voteCast")
            vote_party = member.get("voteParty")
            vote_state = member.get("voteState")

            if not all([bioguide_id, vote_cast]):
                fail_count += 1
                continue

            # Map vote cast to VotePosition enum
            vote_position = None
            if vote_cast in ["Yea", "Aye"]:
                vote_position = "YEA"
                total_yea += 1
            elif vote_cast == "Nay":
                vote_position = "NAY"
                total_nay += 1
            elif vote_cast == "Present":
                vote_position = "PRESENT"
                total_present += 1
            elif vote_cast == "Not Voting":
                vote_position = "NOT_VOTING"
                total_not_voting += 1
            else:
                fail_count += 1
                continue

            # Look up member in cache
            congress_member = member_cache.get(bioguide_id)

            if not congress_member:
                logger.warning(f"Congress member {bioguide_id} not found")
                fail_count += 1
                continue

            # Skip if already exists
            if congress_member.id in existing_member_ids:
                success_count += 1
                continue

            # Add to batch insert
            votes_to_insert.append(
                {
                    "voteId": vote_id,
                    "memberId": congress_member.id,
                    "votePosition": vote_position,
                    "party": vote_party,
                    "state": vote_state,
                }
            )

        # Batch insert all member votes
        if votes_to_insert:
            await prisma.membervote.create_many(data=votes_to_insert)
            success_count += len(votes_to_insert)

        # Update vote totals in the Vote table
        await prisma.vote.update(
            where={"id": vote_id},
            data={
                "totalYea": total_yea,
                "totalNay": total_nay,
                "totalPresent": total_present,
                "totalNotVoting": total_not_voting,
                "totalVoting": total_yea + total_nay,
            },
        )

        logger.info(
            f"Vote {vote_id}: {success_count} member votes inserted, {fail_count} failed. "
            f"Totals: Y:{total_yea} N:{total_nay} P:{total_present} NV:{total_not_voting}"
        )
        return (success_count, fail_count)

    except Exception as e:
        print(f"shit went wrong {e}")
        logger.error(f"fatal error in inserting member votes: {e}")
        return None
