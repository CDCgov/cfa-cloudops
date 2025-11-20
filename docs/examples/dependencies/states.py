import argparse
import os
import random

import pandas as pd


def main(state: str) -> None:
    df = pd.read_csv("us_pop_by_state.csv")
    state_data = df[df["state_code"] == state]
    if not state_data.empty:
        population = state_data.iloc[0]["2020_census"]
        affected = int(population * random.uniform(0.01, 0.5))
        output = pd.DataFrame(
            {"state": [state], "population": [population], "affected": [affected]}
        )
        print(output)
        if os.path.exists("data") is False:
            os.mkdir("data")
        output.to_csv(f"data/{state}_affected.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process state population data.")
    parser.add_argument("-s", "--state", type=str, help="State abbreviation to process")
    args = parser.parse_args()
    main(args.state)
