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
