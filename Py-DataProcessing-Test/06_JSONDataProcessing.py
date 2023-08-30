# Given a JSON file named "data.json" containing a list of people with names and ages,
# write a function to find the oldest person's name.
import json
import pandas as pd

def find_oldest_person(file_path):
    df = pd.read_json(file_path)
    df2 = df.sort_values(by='age', ascending=False)
    return df2.iloc[0]['name'] #Return Single Value of Dataframe

file_path = "data.json"
print(find_oldest_person(file_path))  # Expected output depends on the content of the JSON file.