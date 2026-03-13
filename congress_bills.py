import asyncio
import requests
import logging
import os
import time
from pathlib import Path
from dotenv import load_dotenv
from variables import VALID_BILL_TYPES
from insert import (
    connect_db,
    disconnect_db,
    upsert_bill_details,
    insert_bill_actions,
    insert_bill_summaries,
    insert_house_vote,
    insert_member_votes,
    prisma,
)
from bill import (
    fetchBillDetails,
    fetchBillActions,
    fetchBillSummaries,
    fetchHouseVoteMembers,
    insert_house_vote,
)

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
TARGET_CONGRESS = 118  # Change this to fetch a different congress
PAGE_SIZE = 250  # Max allowed by the API
CONCURRENCY = 10  # How many bills to process in parallel
RATE_LIMIT_SLEEP = (
    0.1  # Seconds between requests (reduced — concurrency handles pacing)
)
RETRY_SLEEP = 60 * 30  # 30 min backoff on 429
PROGRESS_EVERY = 25  # Print a progress line every N completed bills
LOG_DIR = Path("logs")
COMPLETED_LOG = LOG_DIR / f"completed_bills_{TARGET_CONGRESS}.log"
FAILED_LOG = LOG_DIR / f"failed_bills_{TARGET_CONGRESS}.log"

CONGRESS_API_KEY = os.getenv("CONGRESS_API_KEY")
FORCE_REPROCESS = os.getenv("FORCE_REPROCESS", "false").lower() == "true"

logger = logging.getLogger(__name__)


# ── Resume / checkpoint helpers ───────────────────────────────────────────────


def _load_completed() -> set[str]:
    if not COMPLETED_LOG.exists():
        return set()
    return set(COMPLETED_LOG.read_text().splitlines())


def _mark_completed(name_id: str):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with COMPLETED_LOG.open("a") as f:
        f.write(f"{name_id}\n")


def _mark_failed(name_id: str, reason: str):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with FAILED_LOG.open("a") as f:
        f.write(f"{name_id} | {reason}\n")


def _initialize_checkpoint_logs(force_reprocess: bool) -> set[str]:
    """Load completed checkpoints, or reset run state for a full re-import."""
    if force_reprocess:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        if COMPLETED_LOG.exists():
            COMPLETED_LOG.unlink()
        if FAILED_LOG.exists():
            FAILED_LOG.unlink()
        logger.info("FORCE_REPROCESS enabled: cleared checkpoint logs for a full rerun")
        return set()

    return _load_completed()


# ── HTTP helper ───────────────────────────────────────────────────────────────


async def _get(url: str, params: dict) -> dict | None:
    """Async GET with polite sleep and 429 backoff."""
    await asyncio.sleep(RATE_LIMIT_SLEEP)
    while True:
        try:
            response = await asyncio.to_thread(
                requests.get, url, params=params, timeout=15
            )
            if response.status_code == 429:
                logger.warning(f"Rate limited on {url} — sleeping 30 minutes")
                await asyncio.sleep(RETRY_SLEEP)
                continue
            if response.status_code != 200:
                logger.error(f"HTTP {response.status_code} for {url}")
                return None
            return response.json()
        except Exception as e:
            logger.error(f"Request error for {url}: {e}")
            return None


# ── Pagination (concurrent page fetching) ────────────────────────────────────


async def _fetch_page(base_url: str, offset: int) -> tuple[list[dict], dict] | tuple[None, None]:
    """Fetch a single page of bills."""
    params = {
        "api_key": CONGRESS_API_KEY,
        "format": "json",
        "limit": PAGE_SIZE,
        "offset": offset,
    }
    data = await _get(base_url, params)
    if not data:
        logger.error(f"Failed fetching bill list at offset {offset}")
        return None, None
    return data.get("bills", []), data.get("pagination", {})


async def fetch_all_bills_for_congress() -> list[dict]:
    """
    Fetch all bills for the target congress.
    Iterates through every page in order so it stays paced and can retry
    failed pages without stopping the entire import.
    """
    base_url = f"https://api.congress.gov/v3/bill/{TARGET_CONGRESS}"

    all_bills: list[dict] = []
    offset = 0
    total = None

    while True:
        bills = None
        pagination = None

        for attempt in range(1, 4):
            bills, pagination = await _fetch_page(base_url, offset)
            if bills is not None:
                break
            logger.warning(
                f"Retrying bill list page offset={offset} (attempt {attempt}/3)"
            )
            await asyncio.sleep(RATE_LIMIT_SLEEP * attempt)

        if bills is None:
            logger.error(f"Skipping unrecoverable page at offset {offset}")
            offset += PAGE_SIZE
            if total is not None and offset >= total:
                break
            continue

        if total is None:
            total = pagination.get("count", 0)
            logger.info(f"Total bills available: {total}")

        if not bills:
            if total is None or offset >= total:
                break
            offset += PAGE_SIZE
            continue

        all_bills.extend(bills)
        logger.info(
            f"Fetched page offset={offset} with {len(bills)} bills "
            f"(running total {len(all_bills)}/{total if total is not None else '?'})"
        )

        offset += PAGE_SIZE
        if total is not None and offset >= total:
            break

    logger.info(f"Fetched {len(all_bills)} bills for congress {TARGET_CONGRESS}")
    return all_bills


# ── Per-bill full cycle ───────────────────────────────────────────────────────


async def process_bill(
    bill: dict,
    completed: set[str],
    member_cache: dict,
    semaphore: asyncio.Semaphore,
    counters: dict,
) -> bool:
    """
    Full cycle for one bill, guarded by a semaphore to cap concurrency.
    Updates shared `counters` dict for live progress tracking.
    """
    congress = bill.get("congress")
    bill_type = bill.get("type", "").upper()
    bill_number = bill.get("number")

    if not all([congress, bill_type, bill_number]):
        logger.warning(f"Skipping bill with missing fields: {bill}")
        return False

    if bill_type not in VALID_BILL_TYPES:
        counters["skipped"] += 1
        return False

    name_id = f"{congress}{bill_type}{bill_number}"

    if name_id in completed:
        logger.debug(f"Skipping already-completed: {name_id}")
        counters["skipped"] += 1
        return True

    async with semaphore:
        logger.info(f"Processing {name_id}")
        try:
            # 1. Basic details
            legislation = await fetchBillDetails(bill)
            if not legislation:
                _mark_failed(name_id, "fetchBillDetails failed")
                counters["fail"] += 1
                return False

            # 2. Actions + 3. Summaries — run concurrently
            actions_task = asyncio.create_task(fetchBillActions(bill))
            summaries_task = asyncio.create_task(fetchBillSummaries(bill))
            actions_ok, summaries_ok = await asyncio.gather(
                actions_task, summaries_task
            )

            if not actions_ok:
                logger.warning(f"{name_id}: actions failed, continuing")
            if not summaries_ok:
                logger.warning(f"{name_id}: summaries failed, continuing")

            # 4. House votes
            if bill_type in ("HR", "HJRES", "HRES", "HCONRES"):
                await process_house_votes_for_bill(bill, member_cache)

            _mark_completed(name_id)
            completed.add(name_id)
            counters["success"] += 1
            return True

        except Exception as e:
            logger.error(f"Error processing {name_id}: {e}")
            _mark_failed(name_id, str(e))
            counters["fail"] += 1
            return False
        finally:
            counters["done"] += 1


async def process_house_votes_for_bill(bill: dict, member_cache: dict):
    congress = bill.get("congress")
    bill_type = bill.get("type", "").upper()
    bill_number = bill.get("number")

    url = f"https://api.congress.gov/v3/bill/{congress}/{bill_type}/{bill_number}/house-votes"
    params = {"api_key": CONGRESS_API_KEY, "format": "json"}

    data = await _get(url, params)
    if not data:
        return

    votes = data.get("houseRollCallVotes", [])
    if not votes:
        return

    # Process all votes for this bill concurrently
    async def handle_vote(vote):
        vote_obj = await insert_house_vote(vote)
        if not vote_obj:
            return
        roll_number = vote.get("rollCallNumber")
        session = vote.get("sessionNumber")
        if congress and session and roll_number:
            await fetchHouseVoteMembers(
                vote_obj, congress, session, roll_number, member_cache
            )

    await asyncio.gather(*[handle_vote(v) for v in votes])


# ── Progress reporter ─────────────────────────────────────────────────────────


async def progress_reporter(counters: dict, total: int, stop_event: asyncio.Event):
    """
    Prints a progress bar + ETA every PROGRESS_EVERY completed bills,
    and also on a fixed time interval so the terminal never goes silent.
    """
    start_time = time.monotonic()
    last_done = 0

    while not stop_event.is_set():
        await asyncio.sleep(15)  # check every 15 seconds

        done = counters["done"]
        success = counters["success"]
        fail = counters["fail"]
        skipped = counters["skipped"]

        if done == last_done and not stop_event.is_set():
            # Still alive — print a heartbeat anyway
            elapsed = time.monotonic() - start_time
            logger.info(
                f"[heartbeat] {done}/{total} processed | "
                f"✓ {success}  ✗ {fail}  ~ {skipped} skipped | "
                f"elapsed {elapsed / 60:.1f}m"
            )
            continue

        last_done = done
        elapsed = time.monotonic() - start_time
        rate = done / elapsed if elapsed > 0 else 0
        remaining = (total - done) / rate if rate > 0 else float("inf")

        # ASCII progress bar
        pct = done / total if total else 0
        bars = int(pct * 30)
        bar = "█" * bars + "░" * (30 - bars)

        logger.info(
            f"[{bar}] {pct * 100:5.1f}%  {done}/{total} | "
            f"✓ {success}  ✗ {fail}  ~ {skipped} | "
            f"{rate * 60:.1f} bills/min | "
            f"ETA ~{remaining / 60:.1f}m"
        )


# ── Logger setup ──────────────────────────────────────────────────────────────


def setup_logger():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"congress_bills_{TARGET_CONGRESS}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )


# ── Entry point ───────────────────────────────────────────────────────────────


async def main():
    setup_logger()
    logger.info(
        f"Starting congress {TARGET_CONGRESS} bill import (concurrency={CONCURRENCY})"
    )

    await connect_db()

    logger.info("Loading congress member cache...")
    all_members = await prisma.congressmember.find_many()
    member_cache = {cm.bioguideId: cm for cm in all_members}
    logger.info(f"Loaded {len(member_cache)} members into cache")

    completed = _initialize_checkpoint_logs(FORCE_REPROCESS)
    if FORCE_REPROCESS:
        logger.info("Running in full reprocess mode — no bills will be skipped from checkpoint logs")
    else:
        logger.info(f"Resuming — {len(completed)} bills already completed")

    try:
        bills = await fetch_all_bills_for_congress()

        # Only queue bills that pass the type filter
        eligible = [b for b in bills if b.get("type", "").upper() in VALID_BILL_TYPES]
        logger.info(
            f"{len(eligible)} eligible bills to process "
            f"({len(bills) - len(eligible)} skipped by type filter)"
        )

        semaphore = asyncio.Semaphore(CONCURRENCY)
        counters = {"done": 0, "success": 0, "fail": 0, "skipped": 0}
        stop_event = asyncio.Event()

        # Start the background progress reporter
        reporter = asyncio.create_task(
            progress_reporter(counters, len(eligible), stop_event)
        )

        # Fire all bill tasks concurrently (semaphore caps parallelism)
        tasks = [
            process_bill(bill, completed, member_cache, semaphore, counters)
            for bill in eligible
        ]
        await asyncio.gather(*tasks)

        stop_event.set()
        await reporter

        logger.info(
            f"Congress {TARGET_CONGRESS} complete — "
            f"{counters['success']} succeeded, "
            f"{counters['fail']} failed, "
            f"{counters['skipped']} skipped"
        )

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        await disconnect_db()


if __name__ == "__main__":
    asyncio.run(main())
