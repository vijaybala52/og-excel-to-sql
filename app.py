from sqlalchemy import create_engine, text
import pandas as pd
from datetime import time

# -------------------------------
# 1. CREATE DATABASE CONNECTION
# -------------------------------
engine = create_engine(
    "mysql+mysqlconnector://root:root@localhost/Call_Entryv3"
)

# -------------------------------
# 2. READ EXCEL FILE
# -------------------------------
df = pd.read_excel("Customer Detailst1_for_og.xlsx")

# -------------------------------
# 3. CLEAN COLUMN NAMES
# -------------------------------
df.columns = df.columns.str.strip()

# -------------------------------
# 4. RENAME REQUIRED COLUMNS
# (Excel → SQL)
# -------------------------------
df = df.rename(columns={
    "Company ID": "id",                     # REQUIRED for WHERE
    "Working Hrs Start": "working_hrs_start",
    "Working Hrs End": "working_hrs_end"
})

# -------------------------------
# 5. SAFE EXCEL TIME CONVERTER
# (THIS SOLVES 00:00:00 ISSUE)
# -------------------------------
def safe_excel_time(val):
    if pd.isna(val):
        return None

    # Already correct Python time
    if isinstance(val, time):
        return val

    # Excel numeric time (e.g., 0.375)
    if isinstance(val, (int, float)):
        total_seconds = round(val * 24 * 60 * 60)
        h = total_seconds // 3600
        m = (total_seconds % 3600) // 60
        s = total_seconds % 60
        return time(h, m, s)

    # String time (09:30, 9:30 AM, 18:00)
    try:
        return pd.to_datetime(val).time()
    except Exception:
        return None

# -------------------------------
# 6. APPLY TIME CONVERSION
# -------------------------------
df["working_hrs_start"] = df["working_hrs_start"].apply(safe_excel_time)
df["working_hrs_end"] = df["working_hrs_end"].apply(safe_excel_time)

# -------------------------------
# 7. VERIFY BEFORE UPDATE (MANDATORY)
# -------------------------------
print("CHECKING CONVERTED TIMES:")
print(df[["id", "working_hrs_start", "working_hrs_end"]].head(10))
print(type(df.loc[0, "working_hrs_start"]))

# -------------------------------
# 8. SQL UPDATE QUERY
# -------------------------------
update_query = text("""
    UPDATE companies  
    SET working_hrs_start = :working_hrs_start,
        working_hrs_end = :working_hrs_end
    WHERE id = :id
""")

# -------------------------------
# 9. EXECUTE UPDATE (TRANSACTION)
# -------------------------------
with engine.begin() as conn:
    for _, row in df.iterrows():
        conn.execute(update_query, {
            "working_hrs_start": row["working_hrs_start"],
            "working_hrs_end": row["working_hrs_end"],
            "id": row["id"]
        })

print("✅ WORKING HOURS UPDATED SUCCESSFULLY")



df.columns = df.columns.str.strip()


df = df.rename(columns={
    "Company ID": "id",
    "Customer Name": "name",
    "Street": "street",
    "City": "city",
    "State": "state",
    "PIN": "pin",
    "Route": "route",
    "Zone": "zone",
    "Area": "area",
    "Cluster": "cluster",
    "GSTIN": "gstin",
    "Security Procedure": "security",
    "Weekly_off_start": "weekly_off_start",
    "Weekly_off_end": "weekly_off_end",
    "Working_hrs_start": "working_hrs_start",
    "Working_hrs_end": "working_hrs_end"
})

df["working_hrs_start"] = pd.to_datetime(
    df["working_hrs_start"], errors="coerce"
).dt.time

df["working_hrs_end"] = pd.to_datetime(
    df["working_hrs_end"], errors="coerce"
).dt.time

df.to_sql(
    name="companies",
    con=engine,
    if_exists="append",
    index=False
)
print("Data inserted successfully into the companies table.") 



# -------------------------------------------------------- MACHINES ---------------------------------------


