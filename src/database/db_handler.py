from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, String, Float, DateTime, Integer
from sqlalchemy.orm import sessionmaker
from pathlib import Path
import os
import logging

# Create logs directory if it doesn't exist
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True) 

logging.basicConfig(
    filename=log_dir / "database.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_name="stocks.db"):
        self.db_path = Path("data") / db_name
        self.engine = create_engine(f"sqlite:///{self.db_path}")
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)
        
        # Create data directory if it doesn't exist
        Path("data").mkdir(exist_ok=True)
        logger.info(f"Database initialized at {self.db_path}")
        logging.info(f"Database connection established to {self.db_path}")



def setup_stock_table(db):
    """Creates or updates the stock_bars table"""
    stock_bars = Table(
        "stock_bars", 
        db.metadata,
        Column("symbol", String(10), nullable=False),
        Column("timestamp", DateTime(timezone=False), nullable=False),  # Remove timezone
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float),
        Column("volume", Float),
        Column("trade_count", Integer),
        Column("vwap", Float),
        extend_existing=True
    )
    db.metadata.create_all(db.engine)
    return stock_bars