#!/usr/bin/env python
# coding: utf-

import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
import os 

def main(params):
    print(params)
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url =  params.url
    parquet_ouput = 'output.parquet'
    csv_output_path =  'output.csv'


    os.system(f"curl -o {parquet_ouput} {url}")
   
    if os.path.exists(parquet_ouput):
        print(f"File {parquet_ouput} downloaded succesfully!")
    else:
        print(f"Can not download")   
    parquet_df = pd.read_parquet(parquet_ouput)
    parquet_df.to_csv(csv_output_path,index=False)

# create engine and ingest into postgre
    engine  = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df_iter = pd.read_csv(csv_output_path, iterator=True, chunksize=100000)
    df = next(df_iter)
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    df.head(n=0).to_sql(name=table_name,con=engine,if_exists='replace')
        
    while True: 
        df = next(df_iter)
    
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
        df.to_sql(name=table_name, con = engine, if_exists='append')
        print('inserted another chunk')
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    args = parser.parse_args()
    main(args)
