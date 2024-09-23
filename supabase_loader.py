from supabase import create_client, Client
import pandas as pd
import logging

# Use your actual Supabase URL and API key here
url = "https://mscyninxqyaapnwaeupc.supabase.co"
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1zY3luaW54cXlhYXBud2FldXBjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjI0NDcxMTQsImV4cCI6MjAzODAyMzExNH0.WlbITjjJIT8ycBIKOuTQANBVQSu7f8pkbKdku5WvZ5g"

table_name = "stock_data"

def load_to_supabase(df, table_name, url, api_key):
    """Load data into Supabase."""
    try:
        supabase: Client = create_client(url, api_key)

        # Convert DataFrame to a list of dictionaries
        data = df.to_dict(orient='records')

        # Insert data into Supabase table
        response = supabase.table(table_name).insert(data).execute()

        if response.status_code == 201:
            logging.info("Data loaded to Supabase successfully")
        else:
            logging.error(f"Error loading data to Supabase: {response.data}")
            raise Exception("Data load failed")
    
    except Exception as e:
        logging.error(f"Error loading data to Supabase: {e}")
        raise

df =pd.read_csv("fact_table.csv")
df.rename(columns={'Adj Close': 'adj_close'}, inplace=True)
print(df.columns)
load_to_supabase(df, table_name, url, api_key)
