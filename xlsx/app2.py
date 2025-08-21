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
import numpy as np

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

# Function to remove outliers using IQR method


def remove_outliers_iqr(data, multiplier=1.5):
    """
    Remove outliers using the IQR method
    Args:
        data: pandas Series or array
        multiplier: IQR multiplier (default 1.5)
    Returns:
        filtered data without outliers
    """
    Q1 = data.quantile(0.25)
    Q3 = data.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR

    # Filter data within bounds
    filtered_data = data[(data >= lower_bound) & (data <= upper_bound)]

    logger.info(
        f"📊 Outlier Analysis (IQR method with {multiplier}x multiplier):")
    logger.info(f"   Q1: {Q1:.2f}")
    logger.info(f"   Q3: {Q3:.2f}")
    logger.info(f"   IQR: {IQR:.2f}")
    logger.info(f"   Lower Bound: {lower_bound:.2f}")
    logger.info(f"   Upper Bound: {upper_bound:.2f}")
    logger.info(f"   Original count: {len(data)}")
    logger.info(f"   After removing outliers: {len(filtered_data)}")
    logger.info(f"   Outliers removed: {len(data) - len(filtered_data)}")

    return filtered_data


# Calculate outlier-free averages if balanceVolume exists
balance_no_outliers = None
balance_positive_no_outliers = None
if 'balanceVolume' in df.columns:
    # Remove outliers from all balance data
    balance_no_outliers = remove_outliers_iqr(df['balanceVolume'])

    # Remove outliers from balance >= 3 data only
    positive_balances = df[df['balanceVolume'] >= 3]['balanceVolume']
    if len(positive_balances) > 0:
        balance_positive_no_outliers = remove_outliers_iqr(positive_balances)

# Generate filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"sum_{timestamp}.xlsx"

# Create Excel writer with formatting options
with pd.ExcelWriter(filename, engine='openpyxl') as writer:

    # Create summary sheet
    summary_data = {
        'Metric': [
            'Total Users',
            'Users with Balance >= 3',
            'Users with Balance >= 20',
            'Average Balance (All Users)',
            'Average Balance (Balance >= 3)',
            'Average Balance (No Outliers - 1.5×IQR)',
            'Average Balance (Balance >= 3, No Outliers - 1.5×IQR)',
            'Max Balance',
            'Min Balance',
            'Export Date'
        ],
        'Value': [
            len(df),
            len(df[df['balanceVolume'] >= 3]
                ) if 'balanceVolume' in df.columns else 0,
            len(df[df['balanceVolume'] >= 20]
                ) if 'balanceVolume' in df.columns else 0,
            df['balanceVolume'].mean() if 'balanceVolume' in df.columns else 0,
            df[df['balanceVolume'] >= 3]['balanceVolume'].mean(
            ) if 'balanceVolume' in df.columns and len(df[df['balanceVolume'] >= 3]) > 0 else 0,
            balance_no_outliers.mean() if balance_no_outliers is not None and len(
                balance_no_outliers) > 0 else 0,
            balance_positive_no_outliers.mean() if balance_positive_no_outliers is not None and len(
                balance_positive_no_outliers) > 0 else 0,
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
        f"   Users with Balance >= 3: {len(df[df['balanceVolume'] >= 3])}")
    logger.info(
        f"   Users with Balance >= 20: {len(df[df['balanceVolume'] >= 20])}")
    logger.info(
        f"   Average Balance (All Users): {df['balanceVolume'].mean():.2f}")

    # Add average balance for users with balance >= 3
    users_with_balance = df[df['balanceVolume'] >= 3]
    if len(users_with_balance) > 0:
        avg_balance_positive = users_with_balance['balanceVolume'].mean()
        logger.info(
            f"   Average Balance (Balance >= 3): {avg_balance_positive:.2f}")
    else:
        logger.info(
            f"   Average Balance (Balance >= 3): No users with balance >= 3")

    # Add outlier-free averages
    if balance_no_outliers is not None and len(balance_no_outliers) > 0:
        logger.info(
            f"   Average Balance (No Outliers): {balance_no_outliers.mean():.2f}")

    if balance_positive_no_outliers is not None and len(balance_positive_no_outliers) > 0:
        logger.info(
            f"   Average Balance (Balance >= 3, No Outliers): {balance_positive_no_outliers.mean():.2f}")

    logger.info(f"   Max Balance: {df['balanceVolume'].max():.2f}")

# Show column names for reference
logger.info(f"📋 Available columns: {list(df.columns)}")
