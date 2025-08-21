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


# Create DataFrame
df = pd.read_excel("invite_users_20250821_102354.xlsx")

# Generate filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"sum_{timestamp}.xlsx"

# Create Excel writer with formatting options
with pd.ExcelWriter(filename, engine='openpyxl') as writer:

    # Create summary sheet
    summary_data = {
        'Metric': [
            'Total Users',
            'Users with Balance > 0',
            'Users with Balance >= 20',
            'Average Balance (All Users)',
            'Average Balance (Balance > 0)',
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
            df['balanceVolume'].mean() if 'balanceVolume' in df.columns else 0,
            df[df['balanceVolume'] > 0]['balanceVolume'].mean(
            ) if 'balanceVolume' in df.columns and len(df[df['balanceVolume'] > 0]) > 0 else 0,
            df['balanceVolume'].max() if 'balanceVolume' in df.columns else 0,
            df['balanceVolume'].min() if 'balanceVolume' in df.columns else 0,
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
        f"   Average Balance (All Users): {df['balanceVolume'].mean():.2f}")

    # Add average balance for users with balance > 0
    users_with_balance = df[df['balanceVolume'] > 0]
    if len(users_with_balance) > 0:
        avg_balance_positive = users_with_balance['balanceVolume'].mean()
        logger.info(
            f"   Average Balance (Balance > 0): {avg_balance_positive:.2f}")
    else:
        logger.info(
            f"   Average Balance (Balance > 0): No users with positive balance")

    logger.info(f"   Max Balance: {df['balanceVolume'].max():.2f}")

# Show column names for reference
logger.info(f"📋 Available columns: {list(df.columns)}")
