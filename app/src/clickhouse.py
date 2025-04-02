import os, time
import pandas as pd
from clickhouse_driver import Client
from clickhouse_sqlalchemy import make_session, get_declarative_base
from sqlalchemy import create_engine

def connect_clickhouse(host='localhost', port=9000, database='default', username='default', password=''):
    # Direct connection using clickhouse-driver
    client = Client(
        host=host,
        port=port,
        database=database,
        user=username,
        password=password,
        settings={
                'use_numpy': True,
                'max_memory_usage': 16 * 1024 * 1024 * 1024,  # 16GB
                'max_threads': min(os.cpu_count() - 1, 8)
            }
    )
    
    return client

def clickhouse_query(client, query, params=None):
    """Execute query and return results as DataFrame"""
    start_time = time.time()
    result = client.execute(query, params, with_column_types=True)
    df = pd.DataFrame(result[0], columns=[col[0] for col in result[1]])
    end_time = time.time()
    print(f'Elapsed time: {end_time - start_time:.4f} seconds')
    return df

client = connect_clickhouse(host='localhost', port=9000, database='binance', username='max', password='chongma')




query = '''
SELECT 
    symbol,
    onboard_date,
    delivery_date
FROM bn_perp_symbols

'''

query = '''
SELECT 
    symbol,
    timestamp,
    close_time,
    close
FROM bn_perp_klines
WHERE symbol = 'BTCUSDT'

'''

query = '''
show tables 
'''
result = clickhouse_query(client, query)
print(result)

delete_tables = False

if delete_tables:
    query = '''
    show tables 
    '''
    tables = clickhouse_query(client, query)
    print(tables)

    for table in tables['name']:
        query = f'''
        drop table if exists {table} 
        '''
        result = clickhouse_query(client, query)

    query = '''
    show tables 
    '''
    tables = clickhouse_query(client, query)
    print(tables)





'''
SELECT 
    symbol,
    timestamp,
    close_time,
    close
FROM bn_perp_klines
WHERE symbol = 'BTCUSDT'
'''