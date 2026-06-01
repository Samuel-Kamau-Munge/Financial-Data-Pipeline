import psycopg2

conn = psycopg2.connect(dbname='financial', user='postgres', password='postgres', host='localhost', port=5432)
cur = conn.cursor()
with open('sql/schema.sql', 'r') as f:
    cur.execute(f.read())
conn.commit()
cur.close()
conn.close()
print('Initialized DB schema')
