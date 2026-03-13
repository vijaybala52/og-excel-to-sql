from sqlalchemy import create_engine
import pandas as pd

# ---------------------------------
# 1. DATABASE CONNECTION
# ---------------------------------
engine = create_engine(
    "mysql+mysqlconnector://root:root@localhost/Call_Entryv3"
)

# ---------------------------------
# 2. READ EXCEL
# ---------------------------------
df = pd.read_excel("Customer Detailst1_for_og.xlsx", sheet_name="Machines")

# ---------------------------------
# 3. CLEAN COLUMN NAMES
# ---------------------------------
df.columns = df.columns.str.strip()

print("EXCEL COLUMNS:")
print(df.columns.tolist())

# ---------------------------------
# 4. RENAME ALL EXCEL → SQL COLUMNS
# ---------------------------------
df = df.rename(columns={
    "Company ID": "company_id",
    "Ticket_No": "ticket_no",
    "M/c No": "mc_no",
    "Model": "model",
    "Status": "status",
    "Start Dt": "start_dt",
    "End Dt": "end_dt",
    "Inv_No": "Inv_No",
    "Inv_Dt": "Inv_Dt",
    "Inv_Value": "Inv_Value"
})

# ---------------------------------
# 5. REMOVE AUTO-INCREMENT ID
# ---------------------------------
if "Machine ID" in df.columns:
    df = df.drop(columns=["Machine ID"])

# ---------------------------------
# 6. CONVERT DATE COLUMNS SAFELY
# ---------------------------------
for col in ["start_dt", "end_dt", "Inv_Dt"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

# ---------------------------------
# 7. KEEP ONLY COLUMNS THAT EXIST IN SQL
# ---------------------------------
required_cols = [
    "company_id",
    "ticket_no",
    "mc_no",
    "model",
    "status",
    "start_dt",
    "end_dt",
    "Inv_No",
    "Inv_Dt",
    "Inv_Value"
]

df = df[[c for c in required_cols if c in df.columns]]

# ---------------------------------
# 8. VERIFY BEFORE INSERT
# ---------------------------------
print("\nFINAL DATA PREVIEW:")
print(df.head())
print("\nDTYPES:")
print(df.dtypes)

# ---------------------------------
# 9. INSERT INTO MACHINES TABLE
# ---------------------------------
df.to_sql(
    name="machines",
    con=engine,
    if_exists="append",
    index=False
)

print("\n✅ MACHINES DATA INSERTED SUCCESSFULLY")
