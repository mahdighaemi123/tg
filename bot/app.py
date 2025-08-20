from dotenv import load_dotenv
import re
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from telegram import Bot, Update, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from motor.motor_asyncio import AsyncIOMotorClient
import os

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
    "mongodb://mongo_user:mongo_pass@127.0.0.1:27017/tg?authSource=admin"
)
DB_NAME = "tg"
COLLECTION_NAME = "users"
BOT_TOKEN = os.getenv(
    "BOT_TOKEN")


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


class ValidationError(Exception):
    """Custom validation error"""
    pass


async def send_image_with_message(bot: Bot, chat_id: int, image_path: str, caption: str = ""):
    """
    Send an image with caption and optional keyboard to a Telegram chat

    Args:
        bot (Bot): Telegram Bot instance
        chat_id (int): Chat ID where to send the image
        image_path (str): Path to the image file or file object
        caption (str): Caption for the image
        keyboard (ReplyKeyboardMarkup): Optional custom keyboard

    Returns:
        bool: True if successful, False otherwise
    """

    try:
        with open(image_path, 'rb') as image_file:
            await bot.send_photo(
                chat_id=chat_id,
                photo=image_file,
                caption=caption,
                parse_mode='HTML'
            )
        logger.info(f"Image sent successfully to chat {chat_id}")
        return True

    except FileNotFoundError:
        logger.error(f"Image file not found: {image_path}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


def fa_to_eng_number(text):
    """
    Convert Persian/Farsi digits to English digits

    Args:
        text (str): Input text containing Persian digits

    Returns:
        str: Text with Persian digits replaced by English digits
    """
    persian_digits = '۰۱۲۳۴۵۶۷۸۹'
    english_digits = '0123456789'

    # Create translation table
    translation_table = str.maketrans(persian_digits, english_digits)

    # Apply translation
    return text.translate(translation_table)


class DatabaseManager:
    """Database manager for MongoDB operations"""

    def __init__(self, mongo_url: str, db_name: str, collection_name: str):
        self.mongo_url = mongo_url
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None

    async def connect(self):
        """Initialize database connection"""
        try:
            self.client = AsyncIOMotorClient(self.mongo_url)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            self.setting = self.db.setting

            # Test connection
            await self.client.admin.command('ping')
            logger.info(f"""✅ Database connected successfully""")

            # Create indexes
            await self.create_indexes()

        except Exception as e:
            logger.error(f"""❌ Database connection failed: {e}""")
            raise

    async def save_setting(self, key, value):
        """Save a setting to the database"""
        try:
            # Use upsert to update if exists, insert if not
            await self.setting.update_one(
                {"key": key},
                {"$set": {
                    "key": key,
                    "value": value,
                    "updated_at": datetime.utcnow()
                }},
                upsert=True
            )
            logger.info(f"Setting '{key}' saved successfully")
            return True
        except Exception as e:
            logger.error(f"Error saving setting '{key}': {e}")
            return False

    async def load_setting(self, key, default_value=None):
        """Load a setting from the database"""
        try:
            setting_doc = await self.setting.find_one({"key": key})
            if setting_doc:
                logger.info(f"Setting '{key}' loaded successfully")
                return setting_doc.get("value", default_value)
            else:
                logger.info(
                    f"Setting '{key}' not found, returning default value")
                return default_value
        except Exception as e:
            logger.error(f"Error loading setting '{key}': {e}")
            return default_value

    async def create_indexes(self):
        """Create database indexes"""
        try:
            await self.collection.create_index("chat_id", unique=True)
            logger.info(f"""✅ Database indexes created""")
        except Exception as e:
            logger.warning(f"""⚠️  Index creation warning: {e}""")

    async def get_user_state(self, chat_id: int) -> str:
        """Get current user state from database"""
        try:
            user = await self.collection.find_one({"chat_id": chat_id})
            return user.get("state", States.START) if user else States.START
        except Exception as e:
            logger.error(f"""❌ Failed to get user state for {chat_id}: {e}""")
            return States.START

    async def update_user_data(self, chat_id: int, state: str, data: Optional[Dict[str, Any]] = None):
        """Update user state and data"""
        try:
            update_data = {
                "chat_id": chat_id,
                "state": state,
                "updated_at": datetime.now()
            }

            if data:
                update_data.update(data)

            # Check if user exists
            existing_user = await self.collection.find_one({"chat_id": chat_id})
            if not existing_user:
                update_data["created_at"] = datetime.now()

            await self.collection.update_one(
                {"chat_id": chat_id},
                {"$set": update_data},
                upsert=True
            )

            logger.info(f"""💾 Updated user {chat_id}: state={state}""")

        except Exception as e:
            logger.error(
                f"""❌ Failed to update user data for {chat_id}: {e}""")
            raise

    async def get_user_data(self, chat_id: int) -> Optional[Dict[str, Any]]:
        """Get full user data"""
        try:
            return await self.collection.find_one({"chat_id": chat_id})
        except Exception as e:
            logger.error(f"""❌ Failed to get user data for {chat_id}: {e}""")
            return None

    async def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info(f"""✅ Database connection closed""")


class InputValidator:
    """Input validation utilities"""

    @staticmethod
    def validate_name(name: str) -> str:
        """Validate and clean name input"""
        if not name or not name.strip():
            raise ValidationError(f"""نام نمی‌تواند خالی باشد""")

        clean_name = name.strip()

        if not re.match(r"^[آ-یa-zA-Z\s]{2,30}$", clean_name):
            raise ValidationError(
                f"""نام باید بین ۲ تا ۳۰ کاراکتر باشد و فقط شامل حروف باشد""")

        return clean_name

    @staticmethod
    def validate_phone(phone: str) -> str:
        """Validate and clean phone number"""
        if not phone or not phone.strip():
            raise ValidationError(f"""شماره تلفن نمی‌تواند خالی باشد""")

        clean_phone = re.sub(r'[^\d+]', '', phone.strip())

        if not re.match(r'^\+?\d{10,15}', clean_phone):
            raise ValidationError(
                f"""شماره تلفن معتبر وارد کنید (حداقل ۱۰ رقم)""")

        return clean_phone

    @staticmethod
    def validate_uid(uid: str) -> str:
        """Validate and clean UID"""
        if not uid or not uid.strip():
            raise ValidationError(f"""UID نمی‌تواند خالی باشد""")

        clean_uid = uid.strip()

        if not re.match(r'^[a-zA-Z0-9]{6,20}', clean_uid):
            raise ValidationError(
                f"""UID باید شامل 6 تا 9 عدد انگلیسی باشد""")

        return clean_uid


class TelegramBot:
    """Main Telegram bot class"""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.validator = InputValidator()
        self.bot = None

    async def initialize(self):
        """Initialize the bot instance"""
        self.bot = Bot(token=BOT_TOKEN)
        logger.info(f"""✅ Bot initialized""")

    async def handle_update(self, update: Update):
        """Handle a single update"""
        try:
            if not update.message or not update.effective_chat:
                return

            chat_id = update.effective_chat.id
            message_text = update.message.text if update.message.text else ""

            # Get current state
            current_state = await self.db.get_user_state(chat_id)

            logger.info(
                f"""📨 User {chat_id} | State: {current_state} | Message: '{message_text[:50]}...'""")

            # Handle commands first
            if message_text.startswith('/'):
                await self._handle_command(update, message_text)
                return

            # Route based on state
            state_handlers = {
                States.START: self._handle_start_state,
                States.NAME: self._handle_name_input,
                States.PHONE: self._handle_phone_input,
                States.CAPITAL: self._handle_capital_input,  # Added capital handler
                States.UID: self._handle_uid_input,
                States.WAITING_PAYMENT: self._handle_waiting_payment,
                States.COMPLETED: self._handle_completed_state,
                States.CANCELLED: self._handle_cancelled_state
            }

            handler = state_handlers.get(
                current_state, self._handle_unknown_state)
            await handler(update)

        except Exception as e:
            logger.error(f"""❌ Error in handle_update: {e}""")
            await self._send_error_message(update)

    async def _handle_command(self, update: Update, command: str):
        """Handle bot commands"""
        if command == "/start":
            await self._handle_start_command(update)
        elif command == "/cancel":
            await self._handle_cancel_command(update)
        else:
            await self.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"""❓ دستور ناشناخته. برای راهنمایی /help را ارسال کنید."""
            )

    async def _handle_start_command(self, update: Update):
        """Handle /start command"""
        chat_id = update.effective_chat.id

        await self.db.update_user_data(chat_id, States.NAME, {})

        await self.bot.send_message(
            chat_id=chat_id,
            text=f"""سلام خوش اومدی 🎉
برای دریافت سبد مخصوص لطفا ثبت نام انجام بده

برای ادامه نام خودت رو وارد کن:""",
            reply_markup=ReplyKeyboardRemove()
        )

    async def _handle_cancel_command(self, update: Update):
        """Handle /cancel command"""
        chat_id = update.effective_chat.id

        await self.db.update_user_data(chat_id, States.CANCELLED)

        await self.bot.send_message(
            chat_id=chat_id,
            text=f"""❌ عملیات لغو شد!
برای شروع مجدد /start را ارسال کنید.""",
            reply_markup=ReplyKeyboardRemove()
        )

    async def _handle_start_state(self, update: Update):
        """Handle messages in START state"""
        await self.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"""👋 سلام! برای شروع /start را ارسال کنید."""
        )

    async def _handle_name_input(self, update: Update):
        """Handle name input"""
        chat_id = update.effective_chat.id
        message_text = update.message.text if update.message.text else ""

        try:
            name = self.validator.validate_name(message_text)

            await self.db.update_user_data(chat_id, States.PHONE, {"name": name})

            # Create phone request keyboard
            phone_button = KeyboardButton(
                "📱 اشتراک شماره", request_contact=True)
            reply_markup = ReplyKeyboardMarkup(
                [[phone_button]],
                one_time_keyboard=True,
                resize_keyboard=True
            )

            await self.bot.send_message(
                chat_id=chat_id,
                text=f"""✅ سلام {name}! 👋

حالا شماره همراهت رو با دکمه زیر برای ربات بفرست:""",
                reply_markup=reply_markup
            )

        except ValidationError as e:
            await self.bot.send_message(
                chat_id=chat_id,
                text=f"""❌ {str(e)}
لطفاً دوباره تلاش کنید یا /cancel برای لغو ارسال کنید."""
            )

    async def _handle_phone_input(self, update: Update):
        """Handle phone number input"""
        chat_id = update.effective_chat.id

        try:
            # Get phone from contact or text
            if update.message.contact:
                phone = update.message.contact.phone_number
            else:
                phone = update.message.text if update.message.text else ""

            phone = self.validator.validate_phone(phone)

            # Changed to CAPITAL state
            await self.db.update_user_data(chat_id, States.CAPITAL, {"phone": phone})

            # Create capital selection keyboard
            keyboard = [
                [KeyboardButton("۱- زیر ۱۰ میلیون")],
                [KeyboardButton("۲- ۱۰ تا ۳۰ میلیون")],
                [KeyboardButton("۳- ۳۰ تا ۱۰۰ میلیون")],
                [KeyboardButton("۴- ۱۰۰ تا ۵۰۰ میلیون")],
                [KeyboardButton("۵- بالای ۵۰۰ میلیون")]
            ]
            reply_markup = ReplyKeyboardMarkup(
                keyboard,
                one_time_keyboard=True,
                resize_keyboard=True
            )

            await self.bot.send_message(
                chat_id=chat_id,
                text=f"""✅ شماره شما ثبت شد!

سرمایه مازاد شما چقدر است؟
از دکمه های زیر انتخاب کن""",
                reply_markup=reply_markup
            )

        except ValidationError as e:
            await self.bot.send_message(
                chat_id=chat_id,
                text=f"""❌ {str(e)}
لطفاً شماره معتبر وارد کنید یا /cancel برای لغو ارسال کنید."""
            )

    async def _handle_capital_input(self, update: Update):
        """Handle capital selection input"""
        chat_id = update.effective_chat.id
        message_text = update.message.text if update.message.text else ""

        # Valid capital options
        valid_options = [
            "۱- زیر ۱۰ میلیون",
            "۲- ۱۰ تا ۳۰ میلیون",
            "۳- ۳۰ تا ۱۰۰ میلیون",
            "۴- ۱۰۰ تا ۵۰۰ میلیون",
            "۵- بالای ۵۰۰ میلیون"
        ]

        if message_text in valid_options:
            await self.db.update_user_data(chat_id, States.UID, {"capital": message_text})

            await send_image_with_message(self.bot, chat_id, "./uid.jpg",)

            await self.bot.send_message(
                chat_id=chat_id,
                text=f"""
✅ تبریک اطلاعات شما ثبت شد!

برای دریافت کانال مخصوص سبد VIP باید با لینک مخصوص اوتیس عضو صرافی شده باشید.

اگر قبلا با لینک اوتیس ثبت‌نام کرده اید (طبق تصویر) لطفا UID خود را ارسال کنید:


در غیر این صورت ابتدا با لینک زیر در صرافی ثبت‌نام کن 
🔗 https://www.toobit.com/fa/activity/c/August-deposit?invite_code=Wr5Pbu

آموزش کامل ثبت‌نام و استفاده از صرافی:
🔗 https://t.me/otis_iran/837

سپس UID رو ارسال کن""",
                reply_markup=ReplyKeyboardRemove()
            )

        else:
            # Create capital selection keyboard again
            keyboard = [
                [KeyboardButton("۱- زیر ۱۰ میلیون")],
                [KeyboardButton("۲- ۱۰ تا ۳۰ میلیون")],
                [KeyboardButton("۳- ۳۰ تا ۱۰۰ میلیون")],
                [KeyboardButton("۴- ۱۰۰ تا ۵۰۰ میلیون")],
                [KeyboardButton("۵- بالای ۵۰۰ میلیون")]
            ]
            reply_markup = ReplyKeyboardMarkup(
                keyboard,
                one_time_keyboard=True,
                resize_keyboard=True
            )

            await self.bot.send_message(
                chat_id=chat_id,
                text=f"""❌ لطفاً یکی از گزینه‌های موجود را انتخاب کنید:

سرمایه مازاد شما چقدر است؟""",
                reply_markup=reply_markup
            )

    async def _handle_uid_input(self, update: Update):
        """Handle UID input"""
        chat_id = update.effective_chat.id
        message_text = update.message.text if update.message.text else ""

        message_text = str(message_text).strip()
        message_text = fa_to_eng_number(message_text)

        try:
            uid = self.validator.validate_uid(message_text)

            await self.db.update_user_data(chat_id, States.WAITING_PAYMENT, {"uid": uid})

            # Get user data for summary
            user_data = await self.db.get_user_data(chat_id)
            if not user_data:
                raise Exception("User data not found")

            summary_text = f"""✅ اطلاعات شما کامل ثبت شد!

📋 خلاصه اطلاعات:
👤 نام: {user_data.get('name')}
📱 شماره: {user_data.get('phone')}
🆔 UID: {user_data.get('uid')}

💰 حالا ۲۰ دلار باید موجودی در صرافی شارژ کنی.
بعد از شارژ منتظر بمون، طی چند دقیقه برات فایل وبینار ارسال میشه! ✨

⏰ وضعیت: در انتظار پرداخت"""

            await self.bot.send_message(
                chat_id=chat_id,
                text=summary_text
            )

            logger.info(f"""✅ User {chat_id} completed registration""")

        except ValidationError as e:
            await self.bot.send_message(
                chat_id=chat_id,
                text=f"""❌ {str(e)}
لطفاً UID معتبر وارد کنید یا /cancel برای لغو ارسال کنید."""
            )

    async def _handle_waiting_payment(self, update: Update):
        """Handle messages in waiting payment state"""
        await self.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"""⏳ شما در حالت انتظار هستید.

💰 لطفاً ۲۰ دلار موجودی در صرافی شارژ کنید.
بعد از شارژ، فایل وبینار برای شما ارسال خواهد شد.
(ممکن است چند دقیقه طول بکشد)

برای شروع مجدد /start را ارسال کنید."""
        )

    async def _handle_completed_state(self, update: Update):
        """Handle messages when user has completed"""
        await self.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"""✅ شما قبلاً فرآیند ثبت نام را تکمیل کرده‌اید.
برای شروع مجدد /start را ارسال کنید."""
        )

    async def _handle_cancelled_state(self, update: Update):
        """Handle messages in cancelled state"""
        await self.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"""❌ آخرین عملیات شما لغو شده بود.
برای شروع مجدد /start را ارسال کنید."""
        )

    async def _handle_unknown_state(self, update: Update):
        """Handle unknown states"""
        chat_id = update.effective_chat.id

        logger.warning(
            f"""⚠️  Unknown state for user {chat_id}, resetting to START""")

        await self.db.update_user_data(chat_id, States.START)

        await self.bot.send_message(
            chat_id=chat_id,
            text=f"""❌ خطایی در وضعیت رخ داده است.
برای شروع مجدد /start را ارسال کنید."""
        )

    async def _send_error_message(self, update: Update):
        """Send error message to user"""
        try:
            if update.effective_chat:
                await self.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"""❌ خطایی رخ داده است. لطفاً دوباره تلاش کنید.
در صورت تکرار مشکل /start را ارسال کنید."""
                )
        except Exception as e:
            logger.error(f"""❌ Failed to send error message: {e}""")

    async def get_updates_and_process(self) -> bool:
        """Get updates and process them"""
        try:
            OFFSET = await self.db.load_setting("OFFSET", 0)
            updates = await self.bot.get_updates(
                offset=OFFSET,
                limit=10,
                timeout=10,
                allowed_updates=["message"]
            )

            if updates:
                logger.info(f"""📥 Received {len(updates)} updates""")

                for update in updates:
                    try:
                        await self.handle_update(update)
                        # Update offset to acknowledge this update
                        OFFSET = update.update_id + 1
                    except Exception as e:
                        logger.error(
                            f"""❌ Error processing update {update.update_id}: {e}""")
                        # Still update offset to skip problematic update
                        OFFSET = update.update_id + 1

                await self.db.save_setting("OFFSET", OFFSET)

            return True

        except Exception as e:
            logger.error(f"""❌ Error getting updates: {e}""")
            # Wait a bit before retrying
            await asyncio.sleep(5)
            return False

    async def start_polling(self):
        """Start the manual polling loop"""
        logger.info(f"""🔄 Starting manual polling loop...""")

        while True:
            try:
                success = await self.get_updates_and_process()
                if not success:
                    logger.warning(
                        f"""⚠️  Failed to get updates, retrying...""")
                    await asyncio.sleep(1)
                else:
                    # Small delay to prevent excessive API calls
                    await asyncio.sleep(0.1)

            except KeyboardInterrupt:
                logger.info(f"""🛑 Polling stopped by user""")
                break
            except Exception as e:
                logger.error(f"""❌ Error in polling loop: {e}""")
                await asyncio.sleep(5)

    async def cleanup(self):
        """Cleanup bot resources"""
        if self.bot:
            try:
                # Close bot session if it exists
                session = getattr(self.bot, '_bot', None)
                if session and hasattr(session, 'close'):
                    await session.close()
                logger.info(f"""✅ Bot cleaned up""")
            except Exception as e:
                logger.error(f"""❌ Error cleaning up bot: {e}""")


async def main():
    """Main function"""
    db_manager = None
    telegram_bot = None

    try:
        # Initialize database
        logger.info(f"""🔄 Initializing database...""")
        db_manager = DatabaseManager(MONGO_URL, DB_NAME, COLLECTION_NAME)
        await db_manager.connect()

        # Initialize bot
        logger.info(f"""🔄 Initializing bot...""")
        telegram_bot = TelegramBot(db_manager)
        await telegram_bot.initialize()

        # Start bot
        logger.info(f"""🚀 Starting bot with manual polling...""")
        logger.info(f"""📊 Bot Features:""")
        logger.info(f"""  • Manual update handling with get_updates()""")
        logger.info(f"""  • Database-driven state management""")
        logger.info(f"""  • Input validation""")
        logger.info(f"""  • Error recovery""")
        logger.info(f"""  • Clean shutdown""")
        logger.info(f"""Press Ctrl+C to stop""")

        # Start polling
        await telegram_bot.start_polling()

    except KeyboardInterrupt:
        logger.info(f"""🛑 Bot stopped by user""")
    except Exception as e:
        logger.error(f"""❌ Fatal error: {e}""")

    finally:
        # Cleanup
        logger.info(f"""🔄 Cleaning up...""")

        if telegram_bot:
            try:
                await telegram_bot.cleanup()
            except Exception as e:
                logger.error(f"""❌ Error cleaning up bot: {e}""")

        if db_manager:
            try:
                await db_manager.close()
            except Exception as e:
                logger.error(f"""❌ Error closing database: {e}""")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info(f"""🛑 Program interrupted""")
    except Exception as e:
        logger.error(f"""❌ Fatal error in main: {e}""")
