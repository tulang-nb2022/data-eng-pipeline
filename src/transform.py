import pandas as pd
import numpy as np
from typing import Optional, Dict
from abc import ABC, abstractmethod

class DataTransformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    def validate_data(self, df: pd.DataFrame) -> None:
        """Basic data quality checks"""
        # Check for null values
        null_counts = df.isnull().sum()
        if null_counts.any():
            print("Warning: Found null values:")
            print(null_counts[null_counts > 0])
        
        # Check for duplicate rows
        dupes = df.duplicated().sum()
        if dupes > 0:
            print(f"Warning: Found {dupes} duplicate rows")

class FinancialDataTransformer(DataTransformer):
    def transform(self, data: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, pd.DataFrame]:
        """Transform comprehensive market data"""
        transformed_data = {}
        
        for symbol, symbol_data in data.items():
            # Transform intraday data
            intraday_df = self._transform_intraday(symbol_data['intraday'])
        
            # Transform sentiment data
            sentiment_df = self._transform_sentiment(symbol_data['sentiment'])
        
            # Transform technical indicators
            indicators_df = self._transform_indicators(symbol_data['indicators'])
        
            # Combine all transformed data
            combined_df = self._combine_data(intraday_df, sentiment_df, indicators_df)
            
            transformed_data[symbol] = combined_df
            
        return transformed_data
    
    def _transform_intraday(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform intraday price data"""
        self.validate_data(df)
        df = df.copy()
        
        # Convert all columns to numeric
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = pd.to_numeric(
                    df[col].str.replace('$', '').str.replace(',', ''),
                    errors='coerce'
                )
        
        # Calculate price changes and returns
        df['price_change'] = df['close'].diff()
        df['daily_return'] = df['close'].pct_change()
        
        # Calculate volatility metrics
        df['volatility'] = df['daily_return'].rolling(window=20).std()
        df['high_low_range'] = df['high'] - df['low']
        
        # Calculate moving averages
        df['sma_20'] = df['close'].rolling(window=20).mean()
        df['sma_50'] = df['close'].rolling(window=50).mean()
        
        # Calculate trading volume metrics
        df['volume_ma'] = df['volume'].rolling(window=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_ma']
        
        return df

    def _transform_sentiment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform sentiment data"""
        self.validate_data(df)
        df = df.copy()
        
        # Extract relevant sentiment metrics
        sentiment_cols = ['overall_sentiment_score', 'overall_sentiment_label']
        if all(col in df.columns for col in sentiment_cols):
            df['sentiment_score'] = pd.to_numeric(df['overall_sentiment_score'], errors='coerce')
            df['sentiment_label'] = df['overall_sentiment_label']
            
            # Calculate sentiment trends
            df['sentiment_ma'] = df['sentiment_score'].rolling(window=5).mean()
            df['sentiment_change'] = df['sentiment_score'].diff()
        
        return df

    def _transform_indicators(self, indicators: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Transform technical indicators"""
        combined = pd.DataFrame()
        
        # Process RSI
        if 'rsi' in indicators:
            rsi_df = indicators['rsi'].copy()
            rsi_df['RSI'] = pd.to_numeric(rsi_df['RSI'], errors='coerce')
            combined['rsi'] = rsi_df['RSI']
            
            # Add RSI-based signals
            combined['rsi_overbought'] = combined['rsi'] > 70
            combined['rsi_oversold'] = combined['rsi'] < 30
        
        # Process MACD
        if 'macd' in indicators:
            macd_df = indicators['macd'].copy()
            for col in ['MACD', 'MACD_Signal', 'MACD_Hist']:
                macd_df[col] = pd.to_numeric(macd_df[col], errors='coerce')
            combined['macd'] = macd_df['MACD']
            combined['macd_signal'] = macd_df['MACD_Signal']
            combined['macd_hist'] = macd_df['MACD_Hist']
            
            # Add MACD-based signals
            combined['macd_crossover'] = (combined['macd'] > combined['macd_signal']) & \
                                       (combined['macd'].shift(1) <= combined['macd_signal'].shift(1))
        
        return combined
    
    def _combine_data(self, intraday_df: pd.DataFrame, 
                     sentiment_df: pd.DataFrame, 
                     indicators_df: pd.DataFrame) -> pd.DataFrame:
        """Combine all transformed data into a single DataFrame"""
        # Merge all dataframes on their index (timestamp)
        combined = intraday_df.copy()
        
        if not sentiment_df.empty:
            combined = combined.join(sentiment_df, how='left')
        
        if not indicators_df.empty:
            combined = combined.join(indicators_df, how='left')
        
        # Add derived features
        combined['price_momentum'] = combined['close'].pct_change(periods=5)
        combined['volume_momentum'] = combined['volume'].pct_change(periods=5)
        
        # Add market regime indicators
        combined['trend'] = np.where(combined['sma_20'] > combined['sma_50'], 'uptrend', 'downtrend')
        combined['volatility_regime'] = pd.qcut(combined['volatility'], 
                                              q=3, 
                                              labels=['low', 'medium', 'high'])
        
        return combined

def get_transformer(source: str = 'alphavantage') -> DataTransformer:
    """Factory function to get the appropriate transformer"""
    if source.lower() == 'alphavantage':
        return FinancialDataTransformer()
    else:
        raise ValueError(f"Unknown source: {source}")