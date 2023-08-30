# Given a list of sales transactions, each represented as a dictionary with "product", "price", and "quantity",
# write a function to calculate the total revenue.
import pandas as pd


def calculate_total_revenue(transactions):
    df = pd.DataFrame(transactions)
    df['revenue'] = df['price'] * df['quantity']
    total_revenue = df['revenue'].sum()
    return total_revenue


transactions = [
    {"product": "apple", "price": 0.5, "quantity": 100},
    {"product": "banana", "price": 0.3, "quantity": 150},
    {"product": "orange", "price": 0.4, "quantity": 120}
]
print(calculate_total_revenue(transactions))  # Expected output: 105.0
