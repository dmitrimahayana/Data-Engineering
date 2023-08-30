# Given a list of months and corresponding monthly expenses, use matplotlib to create a bar chart.
import matplotlib.pyplot as plt
import numpy as np


def create_expense_chart(months, expenses):
    m = np.array(months)
    e = np.array(expenses)
    y_pos = np.arange(len(m))

    plt.bar(y_pos, e, align='center', alpha=0.5)
    plt.xticks(y_pos, m)
    plt.ylabel('Expense')
    plt.title('Monthly Expense')

    plt.show()


months = ["January", "February", "March", "April", "May"]
expenses = [1200, 1500, 1300, 1400, 1600]
create_expense_chart(months, expenses)  # Display the bar chart
