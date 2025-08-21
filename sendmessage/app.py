import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any
from telegram import Bot
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Environment Configuration
MONGO_URL = os.getenv(
    "MONGODB_URL",
    "mongodb://mongo_user:mongo_pass@95.217.69.70:3000/tg?authSource=admin"
)
DB_NAME = "tg"
BOT_TOKEN = os.getenv("BOT_TOKEN")


class MessageSender:
    """Message sender for users with waiting payment status"""

    def __init__(self):
        self.client = None
        self.db = None
        self.users_collection = None
        self.invited_collection = None
        self.bot = None

    async def connect_db(self):
        """Initialize database connection"""
        try:
            self.client = AsyncIOMotorClient(MONGO_URL)
            self.db = self.client[DB_NAME]
            self.users_collection = self.db.users
            self.invited_collection = self.db.invited
            self.invite_users = self.db.invite_users  # Add this line

            # Test connection
            await self.client.admin.command('ping')
            logger.info("✅ Database connected successfully")

        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    async def init_bot(self):
        """Initialize bot"""
        try:
            self.bot = Bot(token=BOT_TOKEN)
            logger.info("✅ Bot initialized successfully")
        except Exception as e:
            logger.error(f"❌ Bot initialization failed: {e}")
            raise

    async def is_user_in_invite_users(self, uid: str) -> bool:
        """Check if user with specific UID exists in invite_users collection"""
        try:
            invite_user = await self.invite_users.find_one({"uid": uid})
            return invite_user is not None

        except Exception as e:
            logger.error(
                f"❌ Error checking invite_users status for UID {uid}: {e}")
            return False

    async def get_waiting_payment_users(self) -> List[Dict[str, Any]]:
        """Get users with waiting payment status who have UID"""
        try:
            users = await self.users_collection.find({
                "state": "WAITING_PAYMENT",
                "uid": {"$exists": True, "$ne": None, "$ne": ""}
            }).to_list(None)

            logger.info(
                f"📊 Found {len(users)} users with WAITING_PAYMENT status and UID")
            return users

        except Exception as e:
            logger.error(f"❌ Error fetching waiting payment users: {e}")
            return []

    async def is_user_invited_by_uid(self, uid: str) -> bool:
        """Check if user with specific UID exists in invited collection"""
        try:
            invited_user = await self.invited_collection.find_one({"uid": uid})
            return invited_user is not None

        except Exception as e:
            logger.error(f"❌ Error checking invited status for UID {uid}: {e}")
            return False

    async def send_message_to_user(self, chat_id: int, message: str) -> bool:
        """Send message to a specific user"""
        try:
            await self.bot.send_message(chat_id=chat_id, text=message)
            logger.info(f"✅ Message sent to user {chat_id}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to send message to {chat_id}: {e}")
            return False

    async def add_to_invited(self, chat_id: int, uid: str, user_data: Dict[str, Any]) -> bool:
        """Add user to invited collection with UID"""
        try:
            await self.invited_collection.update_one(
                {"uid": uid},
                {
                    "$set": {
                        "chat_id": chat_id,
                        "uid": uid,
                        "name": user_data.get('name'),
                        "phone": user_data.get('phone'),
                        "capital": user_data.get('capital'),
                        "invited_at": datetime.utcnow(),
                        "status": "reminder_sent"
                    }
                },
                upsert=True
            )
            logger.info(
                f"✅ User {chat_id} with UID {uid} added to invited collection")
            return True

        except Exception as e:
            logger.error(
                f"❌ Failed to add {chat_id} (UID: {uid}) to invited: {e}")
            return False

    async def send_bulk_messages(self):
        """Main function to send messages to eligible users"""
        message_text = """رفیق اوتیس
دوباره uid شما بررسی شد، شما زیرمجموعه لینک اوتیس نمی‌باشید.
لطفا با لینک ثبت‌نام کنید و پس از انتقال دارایی دوباره اقدام کنید

لینک:
https://www.toobit.com/fa/activity/c/August-deposit?invite_code=Wr5Pbu

با تشکر❤️"""

        try:
            # Get users with waiting payment status and UID
            waiting_users = await self.get_waiting_payment_users()

            if not waiting_users:
                logger.info(
                    "📭 No users found with WAITING_PAYMENT status and UID")
                return

            sent_count = 0
            skipped_count = 0
            error_count = 0

            for user in waiting_users:
                chat_id = user.get('chat_id')
                uid = user.get('uid')

                if not chat_id or not uid:
                    logger.warning(
                        f"⚠️ User without chat_id ({chat_id}) or UID ({uid}), skipping")
                    continue

                try:
                    # Check if user with this UID is already invited
                    if await self.is_user_invited_by_uid(uid):
                        logger.info(
                            f"⏭️ User with UID {uid} already invited, skipping")
                        skipped_count += 1
                        continue

                    if await self.is_user_in_invite_users(uid):
                        logger.info(
                            f"⏭️ User with UID {uid} exists in invite_users, skipping")
                        skipped_count += 1
                        continue

                    print("*"*20)
                    print(uid)
                    print("*"*20)

                    # Send message
                    if await self.send_message_to_user(chat_id, message_text):
                        # Add to invited collection with UID
                        await self.add_to_invited(chat_id, uid, user)
                        sent_count += 1

                        # Log user info for tracking
                        logger.info(
                            f"📤 Reminder sent to: {user.get('name', 'Unknown')} (UID: {uid})")

                        # Small delay to prevent rate limiting
                        await asyncio.sleep(0.1)
                    else:
                        error_count += 1

                except Exception as e:
                    logger.error(
                        f"❌ Error processing user {chat_id} (UID: {uid}): {e}")
                    error_count += 1

            # Summary
            logger.info(f"""
📊 Bulk reminder sending completed:
✅ Messages sent: {sent_count}
⏭️ Users skipped (already invited): {skipped_count}
❌ Errors: {error_count}
📝 Total processed: {len(waiting_users)}
            """)

        except Exception as e:
            logger.error(f"❌ Error in bulk message sending: {e}")
            raise

    async def get_statistics(self):
        """Get statistics about users and invitations"""
        try:
            total_users = await self.users_collection.count_documents({})
            waiting_payment = await self.users_collection.count_documents({"state": "WAITING_PAYMENT"})
            users_with_uid = await self.users_collection.count_documents({
                "uid": {"$exists": True, "$ne": None, "$ne": ""}
            })
            total_invited = await self.invited_collection.count_documents({})

            logger.info(f"""
📈 Database Statistics:
👥 Total users: {total_users}
⏳ Waiting payment: {waiting_payment}
🆔 Users with UID: {users_with_uid}
✅ Total invited: {total_invited}
            """)

        except Exception as e:
            logger.error(f"❌ Error getting statistics: {e}")

    async def cleanup(self):
        """Cleanup resources"""
        if self.client:
            self.client.close()
            logger.info("✅ Database connection closed")

        if self.bot:
            try:
                # Close bot session if it exists
                session = getattr(self.bot, '_bot', None)
                if session and hasattr(session, 'close'):
                    await session.close()
                logger.info("✅ Bot cleaned up")
            except Exception as e:
                logger.error(f"❌ Error cleaning up bot: {e}")


async def main():
    """Main function"""
    sender = MessageSender()

    try:
        logger.info("🔄 Starting reminder message sender...")

        # Initialize connections
        await sender.connect_db()
        await sender.init_bot()

        # Show statistics
        await sender.get_statistics()

        # Send messages
        await sender.send_bulk_messages()

        logger.info("✅ Reminder sending completed successfully")

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")

    finally:
        await sender.cleanup()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Program interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error in main: {e}")
