# Given a list of numbers, use the map function and a lambda expression to create a new list
# containing the squares of the odd numbers from the original list.
def square_of_odd(numbers):
    result = list(filter(lambda n: n % 2, numbers))
    result2 = list(map(lambda n: n ** 2, result))
    return result2


numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(square_of_odd(numbers))  # Expected output: [1, 9, 25, 49, 81]
