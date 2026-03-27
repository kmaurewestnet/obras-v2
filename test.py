import os, psycopg2
from dotenv import load_dotenv
load_dotenv()
c = psycopg2.connect(
    host=os.getenv('DB_RECORDS_HOST'),
    user=os.getenv('DB_RECORDS_USER'),
    password=os.getenv('DB_RECORDS_PASS'),
    database='records',
    port=os.getenv('DB_RECORDS_PORT', 5432)
)
r = c.cursor()
r.execute("SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name='naps_obras'")
print("Schema of naps_obras:")
for col in r.fetchall():
    print(col)
