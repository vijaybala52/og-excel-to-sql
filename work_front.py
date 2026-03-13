from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(
    "mysql+mysqlconnector://root:root@localhost/Call_Entryv3"
)

df = pd.read_excel("Work_Front.xlsx")

# clean column names
df.columns = df.columns.str.strip()

# remove unnamed columns
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

# remove empty rows
df.dropna(how="all", inplace=True)

# remove rows with NULL ticket_no (CRITICAL)
df = df[df["ticket_no"].notna()]

# convert date columns
for col in ["date", "scheduled_date"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

print("Final row count:", len(df))
print("Final columns:", df.columns.tolist())

df.to_sql(
    name="work_front",
    con=engine,
    if_exists="append",
    index=False
)

print("✅ Data inserted successfully into work_front")
