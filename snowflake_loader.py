import pandas as pd
import numpy as np
import logging
import snowflake.connector

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')

def load_to_snowflake(df, table_name, conn_params):
    """Load data into Snowflake."""
    conn = None
    cursor = None
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=conn_params['user'],
            password=conn_params['password'],
            account=conn_params['account'],
            warehouse=conn_params['warehouse'],
            database=conn_params['database'],
            schema=conn_params['schema']
        )
        cursor = conn.cursor()
        
        # Create table if not exists
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Date DATE,
            Ticker STRING,
            Open FLOAT,
            High FLOAT,
            Low FLOAT,
            Close FLOAT,
            Volume FLOAT
        );
        """
        cursor.execute(create_table_query)
        
        # Insert data
        for index, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (Date, Ticker, Open, High, Low, Close, Volume) VALUES (
                '{row['Date'].strftime('%Y-%m-%d')}', '{row['Ticker']}', {row['Open']}, {row['High']}, {row['Low']}, {row['Close']}, {row['Volume']}
            );
            """
            cursor.execute(insert_query)
        
        conn.commit()
        logging.info("Data loaded to Snowflake successfully")
    except Exception as e:
        logging.error(f"Error loading data to Snowflake: {e}")
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    df = pd.read_csv("fact_table.csv")

    # Snowflake connection parameters
    conn_params = {
        'user': 'GICHURU',
        'password': 'Kidsnextdoor04',
        'account': 'ZQTBSEQ.UK85486',
        'warehouse': 'PHOENIX_WAREHOUSE',
        'database': 'PHOENIX',
        'schema': 'PUBLIC'
    }
    
    load_to_snowflake(df, "FACT_STOCK_PRICES", conn_params)
