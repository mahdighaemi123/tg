import pandas as pd


def analyze_user_payments(file1, file2):
    # Read files
    df1 = pd.read_excel(file1)
    df2 = pd.read_csv(file2)

    # Convert UIDs to string
    df1['uid'] = df1['uid'].astype(str)
    df2['uid'] = df2['uid'].astype(str)

    # Determine which dataframe has 'state' column
    if 'state' in df1.columns:
        main_df = df1
        secondary_df = df2

    elif 'state' in df2.columns:
        main_df = df2
        secondary_df = df1
        
    else:
        print("Error: No 'state' column found")
        return

    # Get UIDs from secondary dataframe
    secondary_uids = set(secondary_df['uid'])

    # Filter for WAITING_PAYMENT users
    waiting_payment_users = main_df[main_df['state'] == 'WAITING_PAYMENT']

    # Count users based on UID existence
    with_uid = len(waiting_payment_users[waiting_payment_users['uid'].isin(secondary_uids)])
    without_uid = len(waiting_payment_users[~waiting_payment_users['uid'].isin(secondary_uids)])

    print(f"WAITING_PAYMENT با uid موجود: {with_uid}")
    print(f"WAITING_PAYMENT بدون uid موجود: {without_uid}")


# Run the analysis
analyze_user_payments('invite_users.xlsx', 'tg.users.csv')