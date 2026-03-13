from sqlalchemy import create_engine
import pandas as pd


engine = create_engine(
    "mysql+mysqlconnector://root:root@localhost/Call_Entryv3"
)
df = pd.read_excel("cluster.xlsx")
# clean column names
df.columns = df.columns.str.strip()


df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# remove empty rows


df.to_sql(
    name="cluster",
    con=engine,
    if_exists="append",
    index=False
)
print(df.head())