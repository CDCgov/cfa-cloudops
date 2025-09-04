import argparse

# from cfa.cloudops import CloudClient


def hello():
    parser = argparse.ArgumentParser(description="CloudOps parser")
    parser.add_argument(
        "--name", type=str, default="World", help="Name to greet"
    )
    args = parser.parse_args()
    print(f"Hello, {args.name}!")
