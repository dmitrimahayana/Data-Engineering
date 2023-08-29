# Given a string containing words separated by spaces, write a function to reverse the order of the words.
def reverse_words_order(sentence):
    text = sentence.split()
    length = len(text) - 1
    result = ""
    for i in range(len(text)):
        if result == "":
            result = text[length - i]
        else:
            result = result + " " + text[length - i]
    return result


sentence = "Hello, this is a test sentence."
print(reverse_words_order(sentence))  # Expected output: "sentence. test a is this Hello,"
