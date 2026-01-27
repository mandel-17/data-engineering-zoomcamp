import pandas as pd

def main():
    green_tripdata = pd.read_parquet("green_tripdata_2025-11.parquet")
    taxi_zone = pd.read_csv("taxi_zone_lookup.csv")

    print(green_tripdata.head())



if __name__ == "__main__":
    main()
