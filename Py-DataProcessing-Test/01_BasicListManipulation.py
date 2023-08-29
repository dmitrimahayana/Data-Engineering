# Given a list of numbers, write a function to calculate the sum of all even numbers in the list.
def sum_even_numbers(numbers):
    result = 0
    for i in numbers:
        if i % 2 == 0:
            result = result + i
    return result

numbers = [2, 5, 8, 10, 13, 15, 20]
print(sum_even_numbers(numbers))  # Expected output: 40