import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Example script for cfa-cloudops"
    )
    parser.add_argument("--user", default="world")
    args = parser.parse_args()
    print(f"Hello, {args.user}!")
