# Given a text file "input.txt" with multiple lines, write a function to count the number of lines,
# the number of words, and the number of characters in the file.
def analyze_text(file_path):
    f = open(file_path, "r")
    text = f.read()
    char = len(text)
    tokens = len(text.split())
    nlines = text.count('\n') + 1
    return (nlines, tokens, char)


file_path = "input.txt"
print(analyze_text(file_path))  # Expected output: (5, 24, 136)
