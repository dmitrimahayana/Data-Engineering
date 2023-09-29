from itertools import permutations

# Define the inputs
inputs = ["jack", "dmitri", "mac"]

# Generate and print all permutations
for perm in permutations(inputs):
    print('-'.join(map(str, perm)))