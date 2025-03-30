import os # operating system operations like check files existance
from pathlib import Path
import gc # garbage collector
import time # time related utility functions
import getpass
import traceback
import requests

import pandas as pd # data frames wrangling
import numpy as np # fast and vectorized math functions
import scipy as sp # scientific calculation toolkit

import matplotlib.pyplot as plt # MATLAB-like plotting library
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
import plotly.graph_objects as go
from plotly.subplots import make_subplots
plt.rcParams["figure.figsize"] = (14, 12)
plt.rcParams['axes.unicode_minus'] = False  # This line is to ensure the minus sign displays correctly

from dateutil.relativedelta import relativedelta
#import datetime
from datetime import datetime,timedelta,timezone
import math
import re

##########################################################
#from crypto_config import *
import duckdb

def connect_duckdb(dbname='duckdb_crypto.db'):
    # create a tempfile name with random name
    duckdb_temp = dbname
    con = duckdb.connect(duckdb_temp)

    credentials_path = Path(os.getenv('DATABASE_DIR'))/'credentials.txt'
    # another option is to read a file called credentials.txt
    if credentials_path.exists():
        with open(credentials_path) as f:
            print('Using credentials.txt file')
            # read the password
            password = f.readline().strip()
    else:
        print('No credentials.txt file found')
        # write the file
        password = getpass.getpass('Enter password:')
        with open(credentials_path, 'w') as f:
            f.write(password)      
   
    
    # Combining Extensions for Efficient Data Pipeline:
    # FROM (
    #        -- Read local parquet
    #        SELECT * FROM read_parquet('local_data.parquet')
    #        UNION ALL
    #        -- Read from S3
    #        SELECT * FROM read_parquet('s3://bucket/remote_data.parquet')
    #        UNION ALL
    #        -- Read from HTTP
    #        SELECT * FROM read_csv('https://example.com/more_data.csv')
    #    )
    # Allows direct reading from HTTP/HTTPS URLs
    # FROM read_csv('https://example.com/data.csv')
    # FROM read_parquet('s3://bucket/data.parquet')
    # FROM read_parquet([
    #        's3://bucket/data1.parquet',
    #        's3://bucket/data2.parquet'
    #    ])
    con.execute('install httpfs')
    con.execute('load httpfs')
    # Efficient reading of parquet files; Write query results to parquet
    # FROM read_parquet('A_share.dsf.parquet')
    con.execute('install parquet')
    con.execute('load parquet')

    # S3 access
    # Basic S3 configuration  
    #con.execute("set s3_region='us-east-1'")   
    #con.execute("set s3_access_key_id='class'")
    #con.execute("set s3_secret_access_key='{password}'".format(password=password)) 
    
    # Optional: Configure endpoint for non-AWS S3 services
    # For example, MinIO (self-hosted S3-compatible storage) or other S3-compatible services
    #if global_on_university_network:
    #    con.execute("set s3_endpoint='responsibly.servebeer.com:19000'")
    #else:
    #    con.execute("set s3_endpoint='ap.loclx.io:39810'")
    # Warning, this is very slow! local expose is abit slow and probably bouncing data via SG
    #con.execute("set s3_url_style='path'") # or 'vhost'
    #con.execute("set s3_use_ssl= false") # or false for non-SSL
 
    
    # DuckDB handles internal parallelization automatically
    # Manual threading is mainly needed for independent external operations (API calls, file I/O)
    # Configure thread count based on CPU cores
    cpu_cores = os.cpu_count()
    recommended_threads = max(1, min(cpu_cores - 1, 8))
    con.execute(f"SET threads={recommended_threads}")
    con.execute('''pragma memory_limit='16GB' ''') 
    # The memory_limit (16GB in your case) is the maximum RAM that DuckDB can use during query execution
    # Query operations (joins, aggregations, sorting)
    # Temporary results during query processing
    con.execute("PRAGMA enable_progress_bar;")

    return con

def duckdb_query(con, query, params=None):    
    start_time = time.time()
    df = con.execute(query, params).fetchdf() # run a query and returns it to a datafrmae
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'Elapsed time: {elapsed_time:.4f} seconds')    
    return df

##########################################################


