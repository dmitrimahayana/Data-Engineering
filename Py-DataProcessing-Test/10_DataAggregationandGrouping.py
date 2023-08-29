# Given a list of dictionaries representing students with "name", "course", and "grade",
# write a function to calculate the average grade for each course.
import pandas as pd


def average_grade_per_course(students):
    df = pd.DataFrame(students)
    df2 = df.groupby('course', as_index=False)['grade'].mean()
    df2 = df2.sort_values(by='grade', ascending=False)
    result = df2.set_index('course')['grade'].to_dict()
    return result


students = [
    {"name": "Alice", "course": "Math", "grade": 85},
    {"name": "Bob", "course": "Math", "grade": 90},
    {"name": "Carol", "course": "History", "grade": 78},
    {"name": "David", "course": "History", "grade": 88},
    {"name": "Eve", "course": "Math", "grade": 92}
]
print(average_grade_per_course(students))
# Expected output: {'Math': 89.0, 'History': 83.0}
