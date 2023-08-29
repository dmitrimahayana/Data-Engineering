# Given a list of email addresses, write a function to extract and return a list of domain names (without @).
import re


def extract_domains(emails):
    result = []
    regex = r'@[A-Za-z]+\.[A-Za-z]+'
    for i in emails:
        email = re.search(regex, i)
        if email:
            sub_string = email.group()
            result.append(sub_string)
    return result


emails = ["user@example.com", "john.doe@gmail.com", "contact@website.net", "contact@website-net"]
print(extract_domains(emails))  # Expected output: ["example.com", "gmail.com", "website.net"]
