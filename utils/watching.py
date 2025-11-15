import sqlite3
import pandas as pd

# חיבור למסד הנתונים
conn = sqlite3.connect("../data/out/loans.db")

# קריאה של כל הטבלה (או רק חלק ממנה)
df = pd.read_sql("SELECT * FROM loans_clean LIMIT 10;", conn)

# הצגת הנתונים
print(df.head())
