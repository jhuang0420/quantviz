from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame  
from datetime import datetime, timedelta
from database.db_handler import Database, setup_stock_table  # Fixed import path
import pandas as pd
from api.utils import load_alpaca_keys  # Fixed import path
from data.validation import StockDataValidator  # Fixed import path

from sqlalchemy import DateTime, Integer, String


def get_alpaca_client():
    secrets = load_alpaca_keys()
    client = StockHistoricalDataClient(
        api_key=secrets["API_KEY"],
        secret_key=secrets["API_SECRET"]    
    )
    return client

def fetch_stock_bars(symbols, start, end, timeframe=TimeFrame.Day):
    client = get_alpaca_client()
    request = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=timeframe,
        start=start,
        end=end
    )
    result = client.get_stock_bars(request)
    return result.df

def save_to_db(df):
    db = Database()
    stock_table = setup_stock_table(db)
    
    # Create a clean copy of the DataFrame
    df_clean = df.reset_index() if isinstance(df.index, pd.MultiIndex) else df.copy()
    
    # Convert timestamp to timezone-naive datetime
    if 'timestamp' in df_clean.columns:
        df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp']).dt.tz_localize(None)
    
    # Ensure correct data types
    df_clean = df_clean.astype({
        'symbol': 'str',
        'trade_count': 'int64',
        'open': 'float64',
        'high': 'float64',
        'low': 'float64',
        'close': 'float64',
        'volume': 'float64',
        'vwap': 'float64'
    })
    
    # Debug output
    print("\nData being saved to database:")
    print(df_clean.head(2))
    print(f"\nData types:\n{df_clean.dtypes}")
    
    # ===== VALIDATION USING StockDataValidator =====
    validator = StockDataValidator()
    
    try:
        # 1. Validate basic structure
        validator.validate_basic_structure(df_clean)
        
        # 2. Validate values
        value_errors = validator.validate_values(df_clean)
        if value_errors:
            print("⚠️ Validation warnings:")
            for error_type, indices in value_errors.items():
                print(f"- {error_type}: {len(indices)} rows affected")
        
    except (ValueError, TypeError) as e:
        print(f"🛑 Validation failed: {e}")
        raise
    # ===== END VALIDATION ===== #
    
    try:
        with db.engine.connect() as conn:
            # Check for existing data to prevent duplicates
            existing_query = f"""
                SELECT DISTINCT timestamp 
                FROM stock_bars 
                WHERE symbol IN {tuple(df_clean['symbol'].unique())}
                AND timestamp BETWEEN '{df_clean['timestamp'].min()}' AND '{df_clean['timestamp'].max()}'
            """
            existing_dates = pd.read_sql(existing_query, conn)
            
            if not existing_dates.empty:
                df_clean = df_clean[~df_clean['timestamp'].isin(existing_dates['timestamp'])]
                print(f"⚠️ Filtered out {len(existing_dates)} duplicate records")
            
            df_clean.to_sql(
                name="stock_bars",
                con=conn,
                if_exists="append",
                index=False,
                dtype={
                    'timestamp': DateTime(),
                    'symbol': String(10),
                    'trade_count': Integer()
                }
            )
        print("✅ Data successfully saved to database")
        
        # Verify the saved data
        with db.engine.connect() as conn:
            saved_data = pd.read_sql("SELECT * FROM stock_bars ORDER BY timestamp DESC LIMIT 5", conn)
            print("\nMost recent records in database:")
            print(saved_data)
            
    except Exception as e:
        print(f"⚠️ Database error: {e}")
        raise
         
if __name__ == "__main__":
    data = fetch_stock_bars(
        symbols=["AAPL", "MSFT"],
        start=datetime.now() - timedelta(days=30),
        end=datetime.now()
    )
    save_to_db(data)
    
    db = Database()
    with db.engine.connect() as conn:
        # Check first 5 rows
        sample = pd.read_sql("SELECT * FROM stock_bars LIMIT 5", conn)
        print("\nSample data from database:")
        print(sample)
    
        # Check row count
        count = pd.read_sql("SELECT COUNT(*) FROM stock_bars", conn)
        print(f"\nTotal rows: {count.iloc[0,0]}")
        
        latest = pd.read_sql("""
            SELECT symbol, timestamp, close 
            FROM stock_bars 
            ORDER BY close DESC 
            LIMIT 10
        """, conn)
        print(latest)
        
        schema = pd.read_sql("PRAGMA table_info(stock_bars)", conn)
        print(schema[['name', 'type']])
        
        