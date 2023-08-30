# Given a list of numbers, use list comprehension to create a new list containing
# the squares of even numbers from the original list.
import numpy as np
import pandas as pd
def square_of_even(numbers):
    df = pd.DataFrame(numbers, columns=["numbers"])
    df2 = df[df['numbers'] % 2 == 0]
    df2 = df2.loc[:, ['numbers']]  # This is to avoid warning
    df2['square'] = df['numbers'] ** 2
    result = df2['square'].values
    return result.tolist() # Return as a List


numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(square_of_even(numbers))  # Expected output: [4, 16, 36, 64, 100]
