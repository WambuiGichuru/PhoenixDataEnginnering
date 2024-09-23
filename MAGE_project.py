import requests
import pandas as pd
import yfinance as yf

def get_polygon_tickers(api_key: str):
    """Extract a list of active tickers from the Polygon API.
    Parameters:
        api_key, type, activity, limit, market.
    Returns:
        List of active tickers
    """
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "active": "true",
        "type": "CS",
        "market": "stocks",
        "limit": 10,
        "apiKey": api_key
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status() 
        data = response.json()
        return data.get('results', [])
    except requests.exceptions.RequestException as e:
        print(f"Error {e} while fetching data from {url}")
        return []

def fetch_yfinance_data(ticker: str, start_date: str, end_date: str):
    """Fetch historical daily stock data from yfinance.
    Parameters:
        ticker: Ticker symbol
        start_date: Start date in the format 'yyyy-mm-dd'
        end_date: End date in the format 'yyyy-mm-dd'
    Returns:
        DataFrame containing stock prices for the ticker
    """
    try:
        data = yf.download(ticker, start=start_date, end=end_date)
        if not data.empty:
            data['Ticker'] = ticker 
            return data
        else:
            print(f"No data found for {ticker}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error {e} while fetching data for {ticker}")
        return pd.DataFrame()

if __name__ == "__main__":
    api_key = "1dkunxVwQcfXrTiIa2scPjHZVZVY2zbQ" 
    tickers_data = get_polygon_tickers(api_key)
    
    if not tickers_data:
        print("No tickers were extracted.")
    else:
        #Extract the ticker symbols from the 'results' list
        tickers = [ticker['ticker'] for ticker in tickers_data]
        print(f"Extracted {len(tickers)} tickers: {tickers}")
        
        ticker = "AAPL"
        start_date = "2010-12-31"
        end_date = "2020-12-31"
        
        stock_data = fetch_yfinance_data(ticker, start_date, end_date)
        #Save the data to a CSV file
        stock_data.to_csv("stock_data.csv")
        print(f"Data extraction complete. Data saved to 'stock_data.csv'.")


#TRANSFORMATION
import pandas as pd

def clean_data(df):
    """Perform basic and advanced data cleaning on the DataFrame."""
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce') 
    df.dropna(subset=['Date'], inplace=True)
    numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']  #Adjusting column names as needed.
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    #Dropping rows with any missing values after conversion to numeric.
    df.dropna(inplace=True)
    
    # Reset index after dropping rows.
    df.reset_index(drop=True, inplace=True)
    
    return df

def generate_date_dimension(start_date, end_date):
    """Generate a DataFrame with date dimensions."""
    dates = pd.date_range(start=start_date, end=end_date)
    date_dim = pd.DataFrame(dates, columns=['Date'])
    date_dim['Year'] = date_dim['Date'].dt.year
    date_dim['Month'] = date_dim['Date'].dt.month
    date_dim['Day'] = date_dim['Date'].dt.day
    return date_dim

if __name__ == "__main__":
    # Load the extracted data
    df = pd.read_csv("stock_data.csv")
    
    # Clean the data
    df = clean_data(df)
    
    # Generate the date dimension
    date_dim = generate_date_dimension("2010-12-31", "2020-12-31")
    
    # Save the transformed data
    df.to_csv("cleaned_stock_data.csv", index=False)
    date_dim.to_csv("date_dimension.csv", index=False)

    print("Data transformation complete.Cleaned data saved to 'cleaned_stock_data.csv' and date dimension saved to 'date_dimension.csv'.")


#MODELLING TO READY FOR LOADING, CREATION OF SCHEMA
import pandas as pd

def create_fact_table(df, date_dim):
    """Create a fact table by merging stock data with the date dimension.

    Parameters:
        df: DataFrame containing stock data
        date_dim: DataFrame containing the date dimension

    Returns:
        DataFrame representing the fact table
    """
    # Ensure 'Date' column is in datetime format for both DataFrames
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    date_dim['Date'] = pd.to_datetime(date_dim['Date'], errors='coerce')
    
    # Rename the 'Adj Close' column to 'adj_close'
    df.rename(columns={'Adj Close': 'adj_close'}, inplace=True)
    # Merge stock data with date dimension
    fact_table = pd.merge(df, date_dim, on='Date', how='left')
    return fact_table

if __name__ == "__main__":
    # Load data
    df = pd.read_csv("stock_data.csv")
    date_dim = pd.read_csv("date_dimension.csv")

    # Create the fact table
    fact_table = create_fact_table(df, date_dim)

    # Save the fact table to a CSV file
    fact_table.to_csv("fact_table.csv", index=False)
    print(f"Fact table created and saved to 'fact_table.csv'.")


