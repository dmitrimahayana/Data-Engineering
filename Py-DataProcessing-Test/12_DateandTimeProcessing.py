# Given a list of dates in the format "YYYY-MM-DD", write a function to find the earliest date.
import pandas as pd


def find_earliest_date(dates):
    df = pd.DataFrame(dates, columns=["dates"])
    df = df.sort_values(by='dates', ascending=True)
    return df.iloc[0]['dates'] #Return Single Value of Dataframe


dates = ["2023-05-15", "2023-08-29", "2023-04-10", "2023-07-01"]
print(find_earliest_date(dates))  # Expected output: "2023-04-10"
