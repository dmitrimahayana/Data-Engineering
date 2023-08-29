# Given a CSV file named "data.csv" with columns "name" and "score", write a function to calculate the average score.
import csv
import pandas as pd

def calculate_average_score(file_path):
    df = pd.read_csv(file_path)
    df2 = df["score"].mean()
    return df2

file_path = "data.csv"
print(calculate_average_score(file_path))  # Expected output depends on the content of the CSV file.