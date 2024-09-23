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
