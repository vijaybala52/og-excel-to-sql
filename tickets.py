from sqlalchemy import create_engine
import pandas as pd
from datetime import time

engine = create_engine(
    "mysql+mysqlconnector://root:root@localhost/Call_Entryv3"
)

# -------------------------------
# 1. READ TICKETS SHEET
# -------------------------------
df = pd.read_excel(
    "Customer Detailst1_for_og.xlsx",
    sheet_name="Tickets"
)

# -------------------------------
# 2. CLEAN COLUMN NAMES
# -------------------------------
df.columns = df.columns.str.strip()

print("EXCEL COLUMNS:", df.columns.tolist())

# -------------------------------
# 3. RENAME EXCEL → SQL
# -------------------------------
df = df.rename(columns={
    "Company ID": "company_id",
    "Machine ID": "machine_id",
    "Contact ID": "contact_id",
    "Ticket_No": "ticket_no",
    "Date": "date",
    "Start time": "start_time",
    "End  time": "end_time",
    "log By": "log_by",
    "Contact Person": "customer_name",
    "Fault/Issues": "fault",
    "Priority": "priority",
    "Status": "status"
})

# -------------------------------
# 4. DROP EXCEL AUTO ID
# -------------------------------
if "Ticket ID" in df.columns:
    df = df.drop(columns=["Ticket ID"])

# -------------------------------
# 5. CONVERT DATE
# -------------------------------
df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

# -------------------------------
# 6. SAFE TIME CONVERTER (Excel-proof)
# -------------------------------

# -------------------------------
# 7. KEEP ONLY SQL COLUMNS
# -------------------------------
df = df[
    [
        "ticket_no",
        "company_id",
        "machine_id",
        "contact_id",
        "date",
        "start_time",
        "end_time",
        "log_by",
        "customer_name",
        "fault",
        "priority",
        "status",
    ]
]

# -------------------------------
# 8. VERIFY BEFORE INSERT
# -------------------------------
print("\nFINAL DATA PREVIEW:")
print(df.head())
print("\nDTYPES:")
print(df.dtypes)

# -------------------------------
# 9. INSERT INTO TICKETS TABLE
# -------------------------------
df.to_sql(
    name="tickets",
    con=engine,
    if_exists="append",
    index=False
)

print("✅ Tickets data inserted successfully.")
