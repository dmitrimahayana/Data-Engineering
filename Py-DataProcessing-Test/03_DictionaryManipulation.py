# Given a dictionary containing people's names and their ages, write a function to find the average age.
def average_age(people):
    result = 0.0
    for x, y in people.items():
        result = result + y
    return result / len(people)

people = {"Alice": 25, "Bob": 30, "Carol": 28, "David": 22}
print(average_age(people))  # Expected output: 26.25