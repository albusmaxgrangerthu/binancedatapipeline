from utils import *
from clickhouse_driver import Client

def connect_clickhouse():
    """Initialize connection to ClickHouse database"""
    try:
        # Initialize ClickHouse client with settings
        client = Client(
            host=os.getenv('CLICKHOUSE_HOST'),
            port=os.getenv('CLICKHOUSE_PORT'),
            database=os.getenv('CLICKHOUSE_DATABASE'),
            user=os.getenv('CLICKHOUSE_USER'),
            password=os.getenv('CLICKHOUSE_PASSWORD'),
            settings={
                'use_numpy': True,
                'max_memory_usage': 16 * 1024 * 1024 * 1024,  # 16GB
                'max_threads': min(os.cpu_count() - 1, 8)
            }
        )
        
        return client

    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        raise

def clickhouse_query(client, query, params=None):
    """Execute query and return results as DataFrame"""
    start_time = time.time()
    result = client.execute(query, params, with_column_types=True)
    df = pd.DataFrame(result[0], columns=[col[0] for col in result[1]])
    end_time = time.time()
    print(f'Elapsed time: {end_time - start_time:.4f} seconds')
    return df
