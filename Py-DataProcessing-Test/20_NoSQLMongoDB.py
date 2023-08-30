import pandas as pd
import pymongo

try:
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["kafka"]
    mycol = mydb["ksql-stock-stream"]

    query = {'$or': [{'id': "GOTO_2023-07-28"}, {'id': "GOTO_2023-07-26"}]}
    query_result = mycol.find(query)

    df = pd.DataFrame(query_result)
    df = df.drop(['_id'], axis=1)
    df = df.sort_values(by='date', ascending=True)
    print(df.to_csv())
except Exception as e:
    print('Failed to connect to mongodb: ' + str(e))
