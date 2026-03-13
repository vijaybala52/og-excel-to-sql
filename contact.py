from sqlalchemy import create_engine
import pandas as pd    


engine = create_engine("mysql+mysqlconnector://root:root@localhost/Call_Entryv3")
df = pd.read_excel("Customer Detailst1_for_og.xlsx", sheet_name ="Contacts")

# clean column names

df.columns = df.columns.str.strip()


df = df.rename(columns={
    "Company ID": "company_id",
    "Machine ID" : "machine_id",
    "Contact Person": "name",
    "Designation": "designation",
    "Phone": "phone",
    "Email": "email"
})


if "Contact ID" in df.columns:
    df =  df.drop(columns=["Contact ID"])

df = df[["company_id", "machine_id", "name", "designation", "phone", "email"]]

df.to_sql(
    name = "contacts",
    con = engine,
    if_exists = "append",
    index = False
)


print("Data inserted successfully into the Contacts table.")