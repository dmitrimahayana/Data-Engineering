# Given a dictionary containing items and their prices, write a function to find the top N items with the highest prices.
import pandas as pd


def top_n_items(prices, n):
    df = pd.DataFrame([prices]).transpose()
    df['row_number'] = df.reset_index().index
    df['prices'] = df[0]
    df['items'] = df.index
    df = df.set_index('row_number')
    df = df[['items', 'prices']].sort_values(by='prices', ascending=False)
    result = list(tuple(x) for x in df.head(n).to_numpy())
    print(result)


prices = {"apple": 0.5, "banana": 0.3, "orange": 0.4, "grape": 0.6, "pear": 0.45}
n = 3
print(top_n_items(prices, n))  # Expected output: [('grape', 0.6), ('apple', 0.5), ('pear', 0.45)]
