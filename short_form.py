
import pandas as pd


df = pd.read_excel("cluster.xlsx",sheet_name="short_form ")

df.to_json("short_form.json", orient="records", date_format="iso")
print("✅ cluster.xlsx has been converted to short_form.json")