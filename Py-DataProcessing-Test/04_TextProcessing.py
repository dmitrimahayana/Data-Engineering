# Write a function that takes a sentence as input and returns the count of each unique word in the sentence.
def word_count(sentence):
    splitter = sentence.split(" ")
    result = {}
    for i in splitter:
        if i in result:
            result[i] = result[i] + 1
        else:
            result[i] = 1
    print(result)

sentence = "This is a test. This test is a good test."
print(word_count(sentence))  # Expected output: {'This': 2, 'is': 2, 'a': 2, 'test.': 1, 'test': 3, 'good': 1}
