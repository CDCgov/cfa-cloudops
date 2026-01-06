import os


def foo():
    message = os.getenv("APP_MESSAGE", "Hello from cfa-cloudops!")
    return message


if __name__ == "__main__":
    print(foo())
