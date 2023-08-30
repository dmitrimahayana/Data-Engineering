# Given a list of words, write a function to filter out words that have fewer than 5 characters.
def filter_long_words(words):
    result = []
    for i in words:
        if len(i) >= 5:
            result.append(i)
    return result

words = ["apple", "banana", "grape", "kiwi", "pear", "orange"]
print(filter_long_words(words))  # Expected output: ['apple', 'banana', 'orange']