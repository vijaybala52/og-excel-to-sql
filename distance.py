import pandas as pd
from sqlalchemy import create_engine

df_temp = pd.read_excel('Distpy.xlsx', sheet_name='Time', nrows=3)
print("First 3 rows of Excel (to understand structure):")
print(df_temp)


df = pd.read_excel('Distpy.xlsx', sheet_name='Time', header=0, index_col=0)

df.columns = df.columns.astype(str)
# Convert index to string to preserve location names  
df.index = df.index.astype(str)

# Remove 'Unnamed' columns if any
df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]

print("Column names (to_location):", df.columns.tolist()[:10])  
print("Index names (from_location):", df.index.tolist()[:10])    # Show first 10

# Clean column names         
df.columns.name = None
df.index.name = None


df = df.replace('--', pd.NA)
df = df.apply(pd.to_numeric, errors='coerce')

# Remove columns that are all NaN
df = df.dropna(how='all', axis=1)
# Remove rows that are all NaN
df = df.dropna(how='all', axis=0)

# Fill remaining NaN values with 0
df = df.fillna(0)

print("Matrix format preview (wide):")
print(df.head())
print(f"\nMatrix shape: {df.shape} (rows x columns)")


df_long = df.stack().reset_index()
df_long.columns = ['from_location', 'to_location', 'Time_mins']


df_long = df_long[df_long['Time_mins'] > 0]

print("\n" + "="*50)
print("Table format (long) - Ready for Database:")
print("="*50)
print("this",df_long.head(20))
print(f"\nTotal rows: {len(df_long)}")
print(f"Columns: {df_long.columns.tolist()}")

# 3. Connect & Upload to MySQL
engine = create_engine('mysql+mysqlconnector://root:root@localhost/call_entryv3')

# Create table if needed
df_long.to_sql('distance_time', engine, if_exists='replace', index=False)

print(" Uploaded to MySQL 'distance_time' table")
