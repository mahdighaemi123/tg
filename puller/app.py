from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder
import logging
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import os
from datetime import datetime
import pandas as pd
import hmac
import hashlib
import requests
import time
from pymongo import UpdateOne

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
BASE_URL = os.getenv("BASE_URL")
MONGODB_URL = os.getenv(
    "MONGODB_URL", "mongodb://mongo_user:mongo_pass@127.0.0.1:27017/tg?authSource=admin")
DATABASE_NAME = "tg"
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class States:
    """Bot states enum-like class"""
    START = "START"
    NAME = "NAME"
    PHONE = "PHONE"
    CAPITAL = "CAPITAL"
    UID = "UID"
    WAITING_PAYMENT = "WAITING_PAYMENT"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


class APIClient:
    """Handles API communication with the external service"""

    def __init__(self, api_key: str, secret_key: str, base_url: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = base_url

    def generate_signature(self, query_string: str) -> str:
        """Generate HMAC signature for API requests"""
        return hmac.new(
            self.secret_key.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    def get_server_time(self):
        """Get server timestamp from API"""
        try:
            r = requests.get(
                f"{self.base_url}/api/v1/time",
                timeout=5
            )
            timestamp = r.json().get("serverTime", -1)
            print(f"Server timestamp: {timestamp}")
            return timestamp
        except Exception as e:
            logger.error(f"❌ Failed to get server time: {e}")
            return int(time.time() * 1000)  # Fallback to local time

    async def fetch_all_users_until_no_new(self, page_size: int = 100, invite_db_manager=None) -> list:
        """
        STEP 1: Fetch all users from API until no new users are found
        This is the optimized version that stops when it encounters existing users
        """
        all_new_users = []
        page_index = 1
        consecutive_existing_count = 0
        max_consecutive_existing = 20  # Stop after 20 consecutive existing users

        logger.info(
            "🔄 STEP 1: Starting to fetch users from API until no new users found...")

        try:
            while True:
                logger.info(
                    f"📄 Fetching page {page_index} (page size: {page_size})")
                timestamp = self.get_server_time()

                params = {
                    "pageIndex": page_index,
                    "pageSize": page_size,
                    "timestamp": timestamp,
                }

                query_string = "&".join(
                    [f"{key}={params[key]}" for key in sorted(params)]
                )
                signature = self.generate_signature(query_string)
                params["signature"] = signature

                headers = {"X-BB-APIKEY": self.api_key}

                response = requests.get(
                    f"{self.base_url}/api/v1/agent/inviteUserList",
                    params=params,
                    headers=headers,
                    timeout=30
                )

                data = response.json()
                response.raise_for_status()

                logger.info(
                    f"📄 Page {page_index} response -> Code: {data.get('code')}, Items: {len(data.get('data', {}).get('list', []))}")

                if "data" not in data or not data["data"].get("list"):
                    logger.info("📄 No more data available from API")
                    break

                items = data["data"]["list"]
                if not items:
                    logger.info("📄 Empty page received, stopping")
                    break

                # Check which users are new
                new_items = []
                existing_count_this_page = 0

                if invite_db_manager:
                    for item in items:
                        user_id = item.get("uid") or item.get("id")
                        if user_id:
                            exists = await invite_db_manager.user_exists(user_id)
                            if not exists:
                                new_items.append(item)
                                consecutive_existing_count = 0  # Reset counter
                            else:
                                existing_count_this_page += 1
                                consecutive_existing_count += 1
                        else:
                            # Add items without ID to be safe
                            new_items.append(item)
                else:
                    new_items = items  # If no DB manager, assume all are new

                logger.info(
                    f"📄 Page {page_index}: {len(new_items)} new users, {existing_count_this_page} existing users")

                all_new_users.extend(new_items)

                # Stop if we've found too many consecutive existing users
                if consecutive_existing_count >= max_consecutive_existing:
                    logger.info(
                        f"🛑 Found {consecutive_existing_count} consecutive existing users. Stopping pagination.")
                    break

                # Stop if no new users on this page
                if len(new_items) == 0:
                    logger.info(
                        "🛑 No new users found on this page. Stopping pagination.")
                    break

                page_index += 1
                time.sleep(0.2)  # Rate limiting

        except Exception as e:
            logger.error(f"❌ Failed to fetch users from API: {e}")
            raise

        logger.info(
            f"✅ STEP 1 COMPLETED: Found {len(all_new_users)} new users to save to database")
        return all_new_users


class InviteUsersDatabase:
    """Manages invite_users collection operations"""

    def __init__(self, mongo_url: str, db_name: str):
        self.client = AsyncIOMotorClient(mongo_url)
        self.db = self.client[db_name]
        self.invite_users = self.db.invite_users

    async def user_exists(self, user_id) -> bool:
        """Check if user exists in invite_users database by uid"""
        try:
            count = await self.invite_users.count_documents({"uid": user_id}, limit=1)
            return count > 0
        except Exception as e:
            logger.error(
                f"❌ Failed to check if user {user_id} exists in invite_users: {e}")
            return False

    async def batch_upsert_users(self, users: list):
        """Batch upsert users for better performance in invite_users collection"""
        try:
            if not users:
                logger.info("💾 No users to save to database")
                return

            logger.info(
                f"💾 Saving {len(users)} users to invite_users database...")

            operations = []
            for user in users:
                user_id = user.get("uid") or user.get("id")
                if not user_id:
                    continue

                operations.append(
                    UpdateOne(
                        {"uid": user_id},
                        {"$set": {**user, "updated_at": datetime.now()}},
                        upsert=True
                    )
                )

            if operations:
                result = await self.invite_users.bulk_write(operations, ordered=False)
                logger.info(
                    f"✅ Database save completed: {result.upserted_count} inserted, {result.modified_count} updated")

        except Exception as e:
            logger.error(
                f"❌ Failed to batch upsert users in invite_users: {e}")
            raise

    async def get_user_by_uid(self, uid) -> dict:
        """Get user from invite_users by UID"""
        try:
            return await self.invite_users.find_one({"uid": uid})
        except Exception as e:
            logger.error(
                f"❌ Failed to get user by UID {uid} from invite_users: {e}")
            return None

    async def create_indexes(self):
        """Create database indexes for better performance in invite_users collection"""
        try:
            await self.invite_users.create_index("uid", unique=True)
            await self.invite_users.create_index("updated_at")
            await self.invite_users.create_index("registerTime")
            await self.invite_users.create_index("balanceVolume")
            logger.info(
                "✅ Database indexes created for invite_users collection")
        except Exception as e:
            logger.error(f"❌ Failed to create indexes for invite_users: {e}")

    async def close(self):
        """Close database connection"""
        self.client.close()


class UsersDatabase:
    """Manages users collection operations (for bot states only)"""

    def __init__(self, mongo_url: str, db_name: str):
        self.client = AsyncIOMotorClient(mongo_url)
        self.db = self.client[db_name]
        self.users = self.db.users

    async def create_indexes(self):
        """Create database indexes for users collection"""
        try:
            await self.users.create_index("chat_id", unique=True)
            await self.users.create_index("uid", unique=False)
            await self.users.create_index("state")
            logger.info("✅ Database indexes created for users collection")
        except Exception as e:
            logger.error(f"❌ Failed to create indexes for users: {e}")
            raise

    async def get_user_state(self, chat_id: int) -> str:
        """Get current user state from users database"""
        try:
            user = await self.users.find_one({"chat_id": chat_id})
            return user.get("state", States.START) if user else States.START
        except Exception as e:
            logger.error(f"❌ Failed to get user state for {chat_id}: {e}")
            return States.START

    async def get_user_by_uid(self, uid: str):
        """Get user by UID from users collection"""
        try:
            return await self.users.find_one({"uid": uid})
        except Exception as e:
            logger.error(f"❌ Failed to get user by UID {uid} from users: {e}")
            return None

    async def get_user_by_chat_id(self, chat_id: int):
        """Get user by chat_id from users collection"""
        try:
            return await self.users.find_one({"chat_id": chat_id})
        except Exception as e:
            logger.error(
                f"❌ Failed to get user by chat_id {chat_id} from users: {e}")
            return None

    async def update_user_state_to_completed(self, chat_id: int, balance_volume: float):
        """Update user state to COMPLETED with balance info"""
        try:
            update_data = {
                "state": States.COMPLETED,
                "balanceVolume": balance_volume,
                "payment_confirmed_at": datetime.now(),
                "updated_at": datetime.now()
            }

            result = await self.users.update_one(
                {"chat_id": chat_id},
                {"$set": update_data}
            )

            if result.modified_count > 0:
                logger.info(
                    f"✅ User {chat_id} state updated to COMPLETED (balance: {balance_volume})")
                return True
            else:
                logger.warning(f"⚠️ User {chat_id} state was not updated")
                return False

        except Exception as e:
            logger.error(
                f"❌ Failed to update user state to COMPLETED for {chat_id}: {e}")
            return False

    async def get_users_in_waiting_payment_state(self) -> list:
        """STEP 2: Get all users in WAITING_PAYMENT state"""
        try:
            cursor = self.users.find({"state": States.WAITING_PAYMENT})
            users = await cursor.to_list(length=None)
            logger.info(f"🔍 Found {len(users)} users in WAITING_PAYMENT state")
            return users
        except Exception as e:
            logger.error(
                f"❌ Failed to get users in WAITING_PAYMENT state: {e}")
            return []

    async def close(self):
        """Close database connection"""
        self.client.close()


class PaymentProcessor:
    """Handles payment verification and state updates"""

    def __init__(self, users_db: UsersDatabase, invite_db: InviteUsersDatabase, telegram_bot=None):
        self.users_db = users_db
        self.invite_db = invite_db
        self.telegram_bot = telegram_bot

    async def process_waiting_payment_users(self):
        """
        STEP 2: Process all users in WAITING_PAYMENT state
        Check if they exist in invite_users with balance >= 20
        If yes, change their state to COMPLETED
        """
        logger.info(
            "🔄 STEP 2: Starting to process users in WAITING_PAYMENT state...")

        # Get all users waiting for payment
        waiting_users = await self.users_db.get_users_in_waiting_payment_state()

        if not waiting_users:
            logger.info("ℹ️ No users found in WAITING_PAYMENT state")
            return

        completed_count = 0

        for bot_user in waiting_users:
            try:
                uid = bot_user.get("uid")
                chat_id = bot_user.get("chat_id")

                if not uid:
                    logger.warning(f"⚠️ User {chat_id} has no UID, skipping")
                    continue

                # Check if user exists in invite_users with sufficient balance
                invite_user = await self.invite_db.get_user_by_uid(str(uid))

                if invite_user:
                    balance_volume = float(invite_user.get("balanceVolume", 0))
                    logger.info(
                        f"🔍 User {uid} found in invite_users with balance: {balance_volume}")

                    if balance_volume >= 20:
                        # Update state to COMPLETED
                        success = await self.users_db.update_user_state_to_completed(
                            chat_id, balance_volume
                        )

                        if success:
                            completed_count += 1

                            # Send notification via Telegram bot if available
                            if self.telegram_bot:
                                await self._send_completion_message(chat_id)

                            logger.info(
                                f"✅ User {uid} payment confirmed and state updated to COMPLETED")
                        else:
                            logger.error(
                                f"❌ Failed to update state for user {uid}")
                    else:
                        logger.info(
                            f"💰 User {uid} balance ({balance_volume}) is less than 20, keeping in WAITING_PAYMENT")
                else:
                    logger.info(
                        f"🔍 User {uid} not found in invite_users database")

            except Exception as e:
                logger.error(
                    f"❌ Error processing user {bot_user.get('uid', 'unknown')}: {e}")
                continue

        logger.info(
            f"✅ STEP 2 COMPLETED: {completed_count} users updated to COMPLETED state")

    async def _send_completion_message(self, chat_id: int):
        """Send completion message to user via Telegram"""
        try:
            text = """
تایید نهایی🎉

لینک کانال مخصوص خدمت شما:
https://t.me/+DKxw_ESgji44MGU0
"""
            await self.telegram_bot.send_message(chat_id=chat_id, text=text)
            logger.info(f"📱 Completion message sent to user {chat_id}")

        except Exception as e:
            logger.error(
                f"❌ Failed to send completion message to {chat_id}: {e}")


async def main():
    """Main function with clear 2-step process"""
    logger.info("🚀 Starting 2-step user processing system...")

    # Initialize components
    api_client = APIClient(API_KEY, SECRET_KEY, BASE_URL)
    invite_db = InviteUsersDatabase(MONGODB_URL, DATABASE_NAME)
    users_db = UsersDatabase(MONGODB_URL, DATABASE_NAME)

    # Initialize Telegram bot if available
    telegram_bot = None
    if BOT_TOKEN:
        try:
            app = ApplicationBuilder().token(BOT_TOKEN).build()
            telegram_bot = app.bot
            logger.info("✅ Telegram bot initialized")
        except Exception as e:
            logger.warning(f"⚠️ Failed to initialize Telegram bot: {e}")

    payment_processor = PaymentProcessor(users_db, invite_db, telegram_bot)

    try:
        # Create database indexes
        await users_db.create_indexes()
        await invite_db.create_indexes()

        while True:
            logger.info("\n" + "="*50)
            logger.info("🔄 Starting new processing cycle...")
            logger.info("="*50)

            # STEP 1: Fetch users from API until no new users found and save to DB
            logger.info(
                "\n🔸 EXECUTING STEP 1: Fetch and save new users from API")
            new_users = await api_client.fetch_all_users_until_no_new(invite_db_manager=invite_db)

            if new_users:
                # Add formatted timestamps
                for user in new_users:
                    if "registerTime" in user:
                        try:
                            ms = int(user["registerTime"])
                            user["registerTimeFormatted"] = datetime.fromtimestamp(
                                ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            user["registerTimeFormatted"] = ""

                # Save to database
                await invite_db.batch_upsert_users(new_users)
                logger.info(
                    f"✅ STEP 1 COMPLETED: {len(new_users)} users processed and saved")
            else:
                logger.info("✅ STEP 1 COMPLETED: No new users found")

            # STEP 2: Process users in WAITING_PAYMENT state
            logger.info("\n🔸 EXECUTING STEP 2: Process waiting payment users")
            await payment_processor.process_waiting_payment_users()

            logger.info("\n✅ Both steps completed successfully!")
            logger.info("⏰ Waiting 10 seconds before next cycle...")

            # Wait before next cycle
            await asyncio.sleep(10)

    except Exception as e:
        logger.error(f"❌ Main process failed: {e}")
        raise

    finally:
        # Cleanup
        await invite_db.close()
        await users_db.close()
        logger.info("🔒 Database connections closed")


if __name__ == "__main__":
    asyncio.run(main())
