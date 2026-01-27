#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "int32",         
    "store_and_fwd_flag": "string",           
    "RatecodeID": "float64",       
    "PULocationID": "int32",         
    "DOLocationID": "int32",         
    "passenger_count": "float64",       
    "trip_distance": "float64",       
    "fare_amount": "float64",       
    "extra": "float64",       
    "mta_tax": "float64",       
    "tip_amount": "float64",       
    "tolls_amount": "float64",       
    "ehail_fee": "float64",       
    "improvement_surcharge": "float64",       
    "total_amount": "float64",       
    "payment_type": "float64",       
    "trip_type": "float64",       
    "congestion_surcharge": "float64",       
    "cbd_congestion_fee": "float64"  
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='pgdatabase', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize):
    """Ingest NYC taxi data into PostgreSQL database."""
    # prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    # url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_parquet(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )

if __name__ == '__main__':
    run()