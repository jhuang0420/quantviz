import schedule
import time
from datetime import datetime, timedelta
from api.fetch_data import fetch_stock_bars, save_to_db

def daily_update():
    print(f"\n{datetime.now()}: Updating data...")
    try:
        data = fetch_stock_bars(
            symbols=["AAPL", "MSFT"],
            start=datetime.now() - timedelta(days=1),
            end=datetime.now()
        )
        save_to_db(data)
    except Exception as e:
        print(f"⚠️ Update failed: {e}")

# Run daily at 6 PM
schedule.every().day.at("18:00").do(daily_update)

while True:
    schedule.run_pending()
    time.sleep(60)