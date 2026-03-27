import pandas as pd
import mysql.connector


df = pd.read_excel("all_data.xlsx", sheet_name="Master")

# Clean column names
df.columns = (
    df.columns.astype(str) # convert to str 
    .str.strip() # remove leading/trailing whitespace
    .str.lower() # convert to lowercase
    .str.replace(r"\s+", "_", regex=True) # replace  spaces with underscores it considers multiple spaces as one
    .str.replace("/", "") # remove forward slashes
)

df = df.dropna(how='all')

print("Columns:", df.columns.tolist())  # debug: print cleaned column names


df = df.rename(columns={
    'customer_name': 'name',            #1
    'address1': 'address1',             #2
    'address2': 'address2',             #3
    'address3': 'address3',             #4
    'country': 'country',               #5
    'gst_no.': 'gstin',                 #6
    'z': 'zone',                        #7
    'a': 'area',                        #8
    'r': 'route',                       #9
    'c': 'cluster',                     #10
    'status': 'status',                 #11
    'model': 'model',                   #12
    'state': 'state',                   #13
    'city': 'city',                     #14
    'pin': 'pin',                       #15
    'security_reqd': 'security',        #16
    'weekly_off': 'weekly_off',         #17
    'contact_person': 'contact_name',   #18
    'contact_no.': 'phone',             #19
    'mc_no': 'mc_no',                   #20
    'inv_date': 'inv_dt',               #21
    'inv_value': 'inv_value',           #22
    'inv_no': 'inv_no',                 #23
    'start_dt': 'start_dt',             #24
    'end_dt': 'end_dt',                 #25
    'designation': 'designation'        #26
})

# Convert NaN to None so MySQL inserts NULL instead of failing on "nan"
# Force object dtype so None is preserved (not coerced back to NaN)
df = df.astype(object).where(pd.notna(df), None)

def clean_value(v):
    return None if pd.isna(v) else v

# 3 Split working hours & weekly off



df['weekly_off_start'] = df['weekly_off']
df['weekly_off_end'] = df['weekly_off']


# 4 MySQL Connection

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="call_entryv7"
)
cursor = conn.cursor()


# 5 Insert Companies

# Ensure required company key fields are present and non-empty
# Drop rows with missing company name to avoid NOT NULL constraint failure.
df['name'] = df['name'].astype(str).str.strip().replace({'nan': None})

companies = df[[
    'name','address1','address2','address3','country','city','state','pin',
    'route','zone','area','cluster','gstin','security',
    'weekly_off_start','weekly_off_end'
]].dropna(subset=['name'])

if companies['name'].isnull().any():
    print("Skipping company rows with empty name:", companies[companies['name'].isnull()])

companies = companies.drop_duplicates(subset=['name','zone','area','route','cluster'])

company_map = {}

for _, row in companies.iterrows():

    cursor.execute("""
        INSERT INTO companies (
            name,address1,address2,address3,country,city,state,pin,
            route,zone,area,cluster,gstin,security,
            weekly_off_start,weekly_off_end
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, tuple(clean_value(v) for v in (
        row['name'], row['address1'], row['address2'], row['address3'],
        row['country'], row['city'], row['state'], row['pin'],
        row['route'], row['zone'], row['area'], row['cluster'],
        row['gstin'], row['security'],
        row['weekly_off_start'], row['weekly_off_end']
    )))

    conn.commit()

    key = (row['name'], row['zone'], row['area'], row['route'], row['cluster'])
    company_map[key] = cursor.lastrowid # Store company_id for later use in machines and contacts


# 6 Insert Machines

machines = df[[
    'name','zone','area','route','cluster','mc_no','model',
    'inv_dt','inv_value','inv_no','start_dt','end_dt','status'
]]

machine_map = {}

for _, row in machines.iterrows():

    key = (row['name'], row['zone'], row['area'], row['route'], row['cluster'])
    c_id = company_map.get(key)
    if c_id is None:
        print("Missing company key for machine:", key, "mc_no", row['mc_no'])
        continue    # or raise error

    cursor.execute("""
        INSERT INTO machines (
            company_id, mc_no, model, status, start_dt, end_dt,
            inv_no, inv_dt, inv_value
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, tuple(clean_value(v) for v in (
        c_id,
        row['mc_no'],
        row['model'],
        row['status'],
        row['start_dt'],
        row['end_dt'],
        row['inv_no'],
        row['inv_dt'],
        row['inv_value']
    )))

    conn.commit()

    machine_map[row['mc_no']] = cursor.lastrowid


# 7 Insert Contacts

contacts = df[[
    'name','zone','area','route','cluster','mc_no',
    'contact_name','designation','phone'
]]#].drop_duplicates(subset=['mc_no','contact_name'])

for _, row in contacts.iterrows():

    key = (row['name'], row['zone'], row['area'], row['route'], row['cluster'])
    c_id = company_map.get(key)
    m_id = machine_map.get(row['mc_no'])

    cursor.execute("""
        INSERT INTO contacts (
            company_id, machine_id, name, designation, phone
        )
        VALUES (%s,%s,%s,%s,%s)
    """, tuple(clean_value(v) for v in (
        c_id,
        m_id,
        row['contact_name'],
        row['designation'],
        row['phone']
    )))

    conn.commit()

# 8 Close connection

cursor.close()
conn.close()

print(" Data Imported Successfully")
