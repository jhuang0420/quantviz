import pandas as pd
from datetime import datetime
from typing import Dict

class StockDataValidator:
    """Validates stock data before database insertion"""
    
    @staticmethod
    def validate_basic_structure(df: pd.DataFrame) -> None:
        """Checks for required columns and basic integrity"""
        required_columns = {
            'symbol': 'object',
            'timestamp': 'datetime64[ns]',
            'open': 'float64',
            'high': 'float64', 
            'low': 'float64',
            'close': 'float64',
            'volume': 'float64'
        }
        
        # Check column existence
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # Check data types (version-compatible)
        type_errors = []
        for col, expected_type in required_columns.items():
            actual_type = str(df[col].dtype)
            if actual_type != expected_type:
                type_errors.append(f"{col} (expected {expected_type}, got {actual_type})")
        
        if type_errors:
            raise TypeError(f"Type mismatches: {', '.join(type_errors)}")
    
    @staticmethod
    def validate_values(df: pd.DataFrame) -> Dict[str, list]:
        """Performs value-level validation"""
        errors = {}
        
        # Price validation (0.01 to $10,000 range)
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            invalid = df[~df[col].between(0.01, 10000)]
            if not invalid.empty:
                errors[f"invalid_{col}"] = invalid.index.tolist()
        
        # Volume validation
        invalid_volume = df[df['volume'] <= 0]
        if not invalid_volume.empty:
            errors["non_positive_volume"] = invalid_volume.index.tolist()
        
        # Timestamp validation
        if 'timestamp' in df.columns:
            future_dates = df[df['timestamp'] > pd.Timestamp.now()]
            if not future_dates.empty:
                errors["future_timestamps"] = future_dates.index.tolist()
        
        return errors