import os, pandas as pd, numpy as np
import matplotlib.pyplot as plt
import pymysql
import sqlalchemy as sa
from sqlalchemy import create_engine, Column, MetaData
import clickhouse_driver
from clickhouse_sqlalchemy import (
    Table, make_session, get_declarative_base, types, engines
)
import getpass,time

global_on_university_network = False # if you're not at university, flag this

#new_host = 'ap.loclx.io'
#new_port = 46749
new_host = '100.65.168.129'
new_port = 9000
user='max'
password='chongma' 

def clickhouse_query(query):
    """
    :param query: the query string
    :return: the query result
    """
    # feed the password for this query
    # read a file that contains the password
    # report timings
    start_time = time.time()
    # only works on the university network i think
    if global_on_university_network:
        db_con = clickhouse_driver.Client(host='responsibly.servebeer.com', port=9000, user=user,
                                        password=password,
                                            database='crsp',
                                            settings={'use_numpy': True})
    # If you are not on the university network, you can use the following
    else:
        print('using the localexpose reversel tunnel')
        db_con = clickhouse_driver.Client(host=new_host, port=new_port, user=user,
                        password=password,
                            database='crsp',
                            settings={'use_numpy': True}) 
    # fix the functon below so that if the result is empty, it returns non
    # instead of an error HELP ME PLEASE
    try:
        df, types = db_con.execute(query,with_column_types=True)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f'Elapsed time: {elapsed_time} seconds')
        # convert df to dataframe
        df2=pd.DataFrame(df)
        # retrieve columns to the dataframe
        df2.columns=[x[0] for x in types]
        return df2
    # need a better solution for an empty result or an error
    # note to self to fix
    except Exception as e:
        print(e)
        return None

print(clickhouse_query('show databases'))