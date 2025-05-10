import streamlit as st
from src.database.db_handler import Database
import pandas as pd

def show_dashboard():
    st.title("Stock Analytics Dashboard")
    
    db = Database()
    with db.engine.connect() as conn:
        data = pd.read_sql("SELECT * FROM stock_bars", conn)
    
    selected_symbol = st.selectbox("Select Symbol", data['symbol'].unique())
    
    symbol_data = data[data['symbol'] == selected_symbol]
    st.line_chart(symbol_data.set_index('timestamp')['close'])

if __name__ == "__main__":
    show_dashboard()