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

    async def fetch_all_users(self, page_size: int = 100, invite_db_manager=None) -> list:
        """Fetch all users from the API with pagination and DB optimization"""
        all_data = []
        page_index = 1
        existing_users_found = 0
        # Stop if we find 10 consecutive existing users
        consecutive_existing_threshold = 10

        try:
            while True:
                print(f"Fetching page {page_index} with page size {page_size}")
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

                print(f"{self.base_url}/api/v1/agent/inviteUserList")
                logger.info(
                    f"Fetching page {page_index} with params: {params}")

                response = requests.get(
                    f"{self.base_url}/api/v1/agent/inviteUserList",
                    params=params,
                    headers=headers,
                    timeout=30
                )

                data = response.json()
                print(f"Response for page {page_index}: {str(data)[:300]}")
                response.raise_for_status()

                logger.info(
                    f"Page {page_index} -> Code: {data.get('code')}, Items: {len(data.get('data', {}).get('list', []))}")

                if "data" not in data or not data["data"].get("list"):
                    break

                items = data["data"]["list"]
                if not items:
                    break

                # Check if items exist in invite_users database (optimization)
                if invite_db_manager:
                    new_items = []
                    page_existing_count = 0

                    for item in items:
                        user_id = item.get("uid") or item.get("id")
                        if user_id:
                            exists = await invite_db_manager.user_exists(user_id)
                            if not exists:
                                new_items.append(item)
                                existing_users_found = 0  # Reset counter when we find new user
                            else:
                                page_existing_count += 1
                                existing_users_found += 1
                                logger.debug(
                                    f"User {user_id} already exists in invite_users DB")
                        else:
                            # Add items without ID to be safe
                            new_items.append(item)

                    logger.info(
                        f"Page {page_index}: {len(new_items)} new users, {page_existing_count} existing users")

                    # Early termination if we've found too many consecutive existing users
                    if existing_users_found >= consecutive_existing_threshold:
                        logger.info(
                            f"Found {existing_users_found} consecutive existing users. Stopping pagination early.")
                        all_data.extend(new_items)
                        break

                    all_data.extend(new_items)

                    # If no new items on this page, we might be done
                    if not new_items:
                        logger.info(
                            "No new users found on this page. Stopping pagination.")
                        break

                else:
                    # Fallback: add all items if no DB manager
                    all_data.extend(items)

                total = int(data["data"].get("total", len(all_data)))
                if len(all_data) >= total:
                    break

                page_index += 1
                time.sleep(0.2)  # Rate limiting

        except Exception as e:
            logger.error(f"❌ Failed to fetch users: {e}")
            raise

        logger.info(f"Total new/updated users to process: {len(all_data)}")
        return all_data


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
            return False  # Assume doesn't exist to be safe

    async def get_existing_uids(self, uids: list) -> set:
        """Get set of existing UIDs from a list of UIDs in invite_users collection"""
        try:
            cursor = self.invite_users.find(
                {"uid": {"$in": uids}},
                {"uid": 1, "_id": 0}
            )
            existing_docs = await cursor.to_list(length=None)
            return {doc["uid"] for doc in existing_docs}
        except Exception as e:
            logger.error(
                f"❌ Failed to get existing UIDs from invite_users: {e}")
            return set()

    async def upsert_user(self, user: dict):
        """Insert or update user by uid in invite_users collection"""
        try:
            user_id = user.get("uid") or user.get("id")
            if not user_id:
                logger.warning("User has no uid or id, skipping")
                return

            await self.invite_users.update_one(
                {"uid": user_id},
                {"$set": {**user, "updated_at": datetime.now()}},
                upsert=True
            )
            logger.debug(f"Upserted user {user_id} in invite_users")

        except Exception as e:
            logger.error(
                f"❌ Failed to upsert user {user.get('uid')} in invite_users: {e}")
            raise

    async def batch_upsert_users(self, users: list):
        """Batch upsert users for better performance in invite_users collection"""
        try:
            if not users:
                return

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
                    f"Batch upsert in invite_users: {result.upserted_count} inserted, {result.modified_count} updated")

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

    async def update_user_data(self, chat_id: int, state: str, data: dict = None):
        """Update user state and data in users collection"""
        try:
            update_data = {
                "chat_id": chat_id,
                "state": state,
                "updated_at": datetime.now()
            }

            if data:
                update_data.update(data)

            # Set created_at for new users
            existing_user = await self.users.find_one({"chat_id": chat_id})
            if not existing_user:
                update_data["created_at"] = datetime.now()

            await self.users.update_one(
                {"chat_id": chat_id},
                {"$set": update_data},
                upsert=True
            )

            logger.info(f"💾 Updated user {chat_id}: state={state}")

        except Exception as e:
            logger.error(f"❌ Failed to update user data for {chat_id}: {e}")
            raise

    async def get_users_in_state(self, state: str) -> list:
        """Get all users in a specific state"""
        try:
            cursor = self.users.find({"state": state})
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"❌ Failed to get users in state {state}: {e}")
            return []

    async def close(self):
        """Close database connection"""
        self.client.close()


class UserProcessor:
    """Processes user data and handles business logic"""

    def __init__(self, users_db: UsersDatabase, invite_db: InviteUsersDatabase, telegram_bot):
        self.users_db = users_db
        self.invite_db = invite_db
        self.telegram_bot = telegram_bot

    @staticmethod
    def convert_timestamp(ms_str: str) -> str:
        """Convert millisecond timestamp to readable format"""
        try:
            ms = int(ms_str)
            return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""

    async def check_user_payment(self, invite_user: dict):
        """Check if user has sufficient balance and notify if needed"""
        try:
            uid = str(invite_user.get("uid"))
            balance_volume = float(invite_user.get("balanceVolume", 0))

            if balance_volume >= 20:
                # Get user from users collection (bot states)
                bot_user = await self.users_db.get_user_by_uid(uid)

                if bot_user and bot_user.get("state") == States.WAITING_PAYMENT:
                    # Update state in users collection
                    await self.users_db.update_user_data(
                        bot_user["chat_id"],
                        States.COMPLETED,
                        {"balanceVolume": balance_volume,
                            "payment_confirmed_at": datetime.now()}
                    )

                    text = """
تایید نهایی🎉

لینک کانال مخصوص خدمت شما:
https://t.me/+DKxw_ESgji44MGU0
"""

                    if self.telegram_bot:
                        await self.telegram_bot.send_message(
                            chat_id=bot_user["chat_id"],
                            text=text
                        )

                    logger.info(
                        f"✅ Payment confirmed for user {uid} (balance: {balance_volume})")

        except Exception as e:
            logger.error(
                f"❌ Failed to check user payment for {invite_user.get('uid')}: {e}")

    async def process_waiting_users(self):
        """Process all users in WAITING_PAYMENT state"""
        try:
            waiting_users = await self.users_db.get_users_in_state(States.WAITING_PAYMENT)

            for user in waiting_users:
                uid = user.get("uid")
                if uid:
                    invite_user = await self.invite_db.get_user_by_uid(uid)
                    if invite_user:
                        await self.check_user_payment(invite_user)

            logger.info(
                f"Processed {len(waiting_users)} users in WAITING_PAYMENT state")

        except Exception as e:
            logger.error(f"❌ Failed to process waiting users: {e}")


async def main():
    """Main function to orchestrate the entire process"""
    logger.info("🚀 Starting user sync process...")

    # Initialize components
    api_client = APIClient(API_KEY, SECRET_KEY, BASE_URL)
    invite_db = InviteUsersDatabase(MONGODB_URL, DATABASE_NAME)
    users_db = UsersDatabase(MONGODB_URL, DATABASE_NAME)

    telegram_bot = None
    if BOT_TOKEN:
        try:
            app = ApplicationBuilder().token(BOT_TOKEN).build()
            telegram_bot = app.bot
            logger.info("✅ Telegram bot initialized")
        except Exception as e:
            logger.warning(f"⚠️ Failed to initialize Telegram bot: {e}")

    user_processor = UserProcessor(users_db, invite_db, telegram_bot)

    try:
        # Create database indexes
        await users_db.create_indexes()
        await invite_db.create_indexes()

        while True:
            # Fetch users from API with DB optimization
            logger.info("📥 Fetching users from API...")
            invite_users = await api_client.fetch_all_users(invite_db_manager=invite_db)

            if invite_users:
                logger.info(
                    f"✅ Found {len(invite_users)} new/updated users to process")

                # Process each user and add formatted timestamp
                for user in invite_users:
                    if "registerTime" in user:
                        user["registerTimeFormatted"] = user_processor.convert_timestamp(
                            user["registerTime"]
                        )

                # Batch upsert for better performance in invite_users collection
                await invite_db.batch_upsert_users(invite_users)

                # Check payment status for newly updated users
                if telegram_bot:
                    for user in invite_users:
                        await user_processor.check_user_payment(user)

                logger.info(
                    f"✅ Successfully processed {len(invite_users)} invite users")
            else:
                logger.info("ℹ️ No new invite users to process")

            # Process all users in WAITING_PAYMENT state (regardless of new API data)
            await user_processor.process_waiting_users()

            # Optional: Save to Excel file
            # try:
            #     if invite_users:
            #         df = pd.DataFrame(invite_users)
            #         df.to_excel("invite_users.xlsx", index=False)
            #         logger.info("💾 Data also saved to invite_users.xlsx")
            # except Exception as e:
            #     logger.warning(f"⚠️ Failed to save Excel file: {e}")

            # Add delay before next iteration
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
