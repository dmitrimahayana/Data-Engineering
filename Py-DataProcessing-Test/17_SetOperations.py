# Given two lists of numbers, write a function to find the common elements between them using sets.
import pandas as pd


def find_common_elements(list1, list2):
    df1 = pd.DataFrame(list1, columns=["numbers"])
    df2 = pd.DataFrame(list2, columns=["numbers"])
    join = pd.merge(df1, df2, on=["numbers", "numbers"])
    result = join["numbers"].values.tolist()
    set_result = set(result)
    return set_result


list1 = [1, 2, 3, 4, 5]
list2 = [3, 4, 5, 6, 7]
print(find_common_elements(list1, list2))  # Expected output: {3, 4, 5}
var = {3, 4, 5}