import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table

DB_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://postgres:postgres@postgres:5432/financial')
SOURCE = os.getenv('SOURCE_FILE', '/app/Accounts-Receivable.xlsx')

print(f"Using DB: {DB_URL}")
print(f"Source file: {SOURCE}")

engine = create_engine(DB_URL)

# read data
if os.path.exists(SOURCE):
    df = pd.read_excel(SOURCE, engine='openpyxl')
else:
    print(f"Source file not found at {SOURCE}, inserting sample rows")
    df = pd.DataFrame([
        {'Status': 'Open', 'Amount': '100.00', 'Customer': 'ACME'},
        {'Status': 'Closed', 'Amount': '50', 'Customer': 'Beta'}
    ])

metadata = MetaData()
raw_table = Table('raw_accounts_receivable', metadata, autoload_with=engine)

with engine.begin() as conn:
    for _, row in df.iterrows():
        # Use lowercase column names for Postgres (unquoted identifiers are folded to lowercase)
        status_val = row.get('Status') if 'Status' in row.index else row.get('status')
        amount_val = row.get('Amount') if 'Amount' in row.index else row.get('amount')
        customer_val = row.get('Customer') if 'Customer' in row.index else row.get('customer')

        conn.execute(raw_table.insert().values(
            status=status_val,
            amount=str(amount_val) if amount_val is not None else None,
            customer=customer_val
        ))

print(f"Inserted {len(df)} rows into raw_accounts_receivable")
