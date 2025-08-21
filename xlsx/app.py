from dotenv import load_dotenv
import logging
import asyncio
import os
from datetime import datetime
import pandas as pd
import hmac
import hashlib
import requests
import time

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
BASE_URL = os.getenv("BASE_URL")

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


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

    async def fetch_all_users_and_save_to_excel(self, page_size: int = 100) -> None:
        """
        Fetch all users from API and save to Excel file
        """
        all_users = []
        page_index = 1

        logger.info("🔄 Starting to fetch users from API...")

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
                    f"📄 Page {page_index} response -> Code: {data.get('code')}, "
                    f"Items: {len(data.get('data', {}).get('list', []))}"
                )

                if "data" not in data or not data["data"].get("list"):
                    logger.info("📄 No more data available from API")
                    break

                items = data["data"]["list"]
                if not items:
                    logger.info("📄 Empty page received, stopping")
                    break

                # Add formatted timestamps and other processing
                for user in items:
                    if "registerTime" in user:
                        try:
                            ms = int(user["registerTime"])
                            user["registerTimeFormatted"] = datetime.fromtimestamp(
                                ms / 1000
                            ).strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            user["registerTimeFormatted"] = ""

                    # Add current fetch timestamp
                    user["fetchedAt"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S")

                all_users.extend(items)
                logger.info(
                    f"📄 Page {page_index} - Total users collected: {len(all_users)}")

                page_index += 1
                time.sleep(0.5)  # Rate limiting

        except Exception as e:
            logger.error(f"❌ Failed to fetch users from API: {e}")
            raise

        logger.info(
            f"✅ Fetching completed: Found {len(all_users)} total users")

        # Save to Excel file
        if all_users:
            await self.save_to_excel(all_users)
        else:
            logger.info("⚠️ No users found to save")

    async def save_to_excel(self, users: list) -> None:
        """Save users data to Excel file using pandas"""
        try:
            if not users:
                logger.info("💾 No users to save")
                return

            logger.info(f"💾 Saving {len(users)} users to Excel file...")

            # Create DataFrame
            df = pd.DataFrame(users)

            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"invite_users_{timestamp}.xlsx"

            # Create Excel writer with formatting options
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Save main data
                df.to_excel(writer, sheet_name='Users', index=False)

                # Create summary sheet
                summary_data = {
                    'Metric': [
                        'Total Users',
                        'Users with Balance > 0',
                        'Users with Balance >= 20',
                        'Average Balance',
                        'Max Balance',
                        'Min Balance',
                        'Export Date'
                    ],
                    'Value': [
                        len(df),
                        len(df[df['balanceVolume'] > 0]
                            ) if 'balanceVolume' in df.columns else 0,
                        len(df[df['balanceVolume'] >= 20]
                            ) if 'balanceVolume' in df.columns else 0,
                        df['balanceVolume'].mean(
                        ) if 'balanceVolume' in df.columns else 0,
                        df['balanceVolume'].max(
                        ) if 'balanceVolume' in df.columns else 0,
                        df['balanceVolume'].min(
                        ) if 'balanceVolume' in df.columns else 0,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ]
                }

                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)

                # Auto-adjust column widths for better readability
                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width

            logger.info(f"✅ Excel file saved successfully: {filename}")

            # Print summary statistics
            logger.info("📊 Data Summary:")
            logger.info(f"   Total Users: {len(df)}")
            if 'balanceVolume' in df.columns:
                logger.info(
                    f"   Users with Balance > 0: {len(df[df['balanceVolume'] > 0])}")
                logger.info(
                    f"   Users with Balance >= 20: {len(df[df['balanceVolume'] >= 20])}")
                logger.info(
                    f"   Average Balance: {df['balanceVolume'].mean():.2f}")
                logger.info(f"   Max Balance: {df['balanceVolume'].max():.2f}")

            # Show column names for reference
            logger.info(f"📋 Available columns: {list(df.columns)}")

        except Exception as e:
            logger.error(f"❌ Failed to save users to Excel: {e}")
            raise


async def main():
    """Main function to fetch data and save to Excel"""
    logger.info("🚀 Starting API data fetcher and Excel exporter...")

    # Validate required environment variables
    if not all([API_KEY, SECRET_KEY, BASE_URL]):
        logger.error(
            "❌ Missing required environment variables (API_KEY, SECRET_KEY, BASE_URL)")
        return

    # Initialize API client
    api_client = APIClient(API_KEY, SECRET_KEY, BASE_URL)

    try:
        # Fetch all users and save to Excel
        await api_client.fetch_all_users_and_save_to_excel(page_size=100)
        logger.info("✅ Process completed successfully!")

    except Exception as e:
        logger.error(f"❌ Process failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
