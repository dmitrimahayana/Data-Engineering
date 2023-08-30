import psycopg2
import pandas as pd

try:
    conn = psycopg2.connect(database="IDX-Stock",
                            user="postgres",
                            host='localhost',
                            password="postgres",
                            port=5432)
    cur = conn.cursor()

    query = 'SELECT * FROM "IDX-Schema".account \
                ORDER BY id ASC '
    cur.execute(query)
    rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=['id', 'name', 'pass', 'email', 'created_on', 'last_login'])
    df2 = df[df['created_on'].dt.strftime('%Y-%m-%d') > '2023-08-28']
    print(df2.iloc[0].to_dict())

except Exception as e:
    print('Failed to connect to postgres: ' + str(e))
