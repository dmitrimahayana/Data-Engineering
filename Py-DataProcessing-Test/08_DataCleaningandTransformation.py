# Given a list of strings with numeric values and some missing values ("NA"),
# write a function to calculate the average of the numeric values, ignoring the missing values.
import pandas as pd


def calculate_average(numbers):
    df = pd.DataFrame(numbers, columns=["numbers"])
    df2 = df[df["numbers"] != "NA"]
    df3 = df2.loc[:, ['numbers']]  # This is to avoid warning
    df3['new_num'] = df3['numbers'].fillna('0').astype(int)
    result = df3["new_num"].mean()
    return result


data = ["12", "NA", "25", "10", "NA", "18"]
print(calculate_average(data))  # Expected output: 16.25
