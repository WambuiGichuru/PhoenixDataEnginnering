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
