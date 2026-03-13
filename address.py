import pandas as pd
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings('ignore')

engine = create_engine("mysql+mysqlconnector://root:root@localhost/Call_Entryv3")

df = pd.read_excel("Customer Detailst1_for_og.xlsx", sheet_name="tblCompany")

df = df.rename(columns={
    'Company ID': 'id',
    'address 1': 'address1',
    'address 2': 'address2', 
    'address 3': 'address3'
})

update_cols = ['id', 'address1', 'address2', 'address3', 'country']
df = df[update_cols].fillna('')
df['id'] = df['id'].astype(int)

print("✅ Data ready:", df.head())

df.to_sql('temp_company_updates', con=engine, if_exists='replace', index=False)

with engine.connect() as conn:
    sql = text("""
        UPDATE companies c
        JOIN temp_company_updates t ON c.id = t.id
        SET 
            c.address1 = t.address1,
            c.address2 = t.address2,
            c.address3 = t.address3,
            c.country = t.country
    """)
    result = conn.execute(sql)
    updated_rows = result.rowcount
    conn.commit()

# ✅ FIXED: Use connection for DROP too
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS temp_company_updates"))
    conn.commit()

print(f"✅ SUCCESS! Updated {updated_rows} companies!")
print("Verify: SELECT id, name, address1, address2, address3, country FROM companies WHERE id <= 5;")
