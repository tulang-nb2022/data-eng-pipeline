# Financial Analytics Models

This directory contains the dbt models for our financial analytics pipeline.

## Structure

- `staging/`: Raw data cleaning and standardization
- `intermediate/`: Technical indicators and metric calculations
- `marts/`: Business-ready data models
  - `core/`: Primary analytical tables
  - `reporting/`: Presentation-ready views

## Key Models

### Technical Indicators (`int_technical_indicators`)
Calculates common technical analysis indicators:
- RSI (14-day)
- MACD
- Bollinger Bands

### Stock Performance (`fct_stock_performance`)
Combines technical and fundamental metrics:
- Price performance
- Risk metrics
- Trading signals

## Update Schedule
- Raw data refreshes daily by 9am EST
- All models rebuild after raw data update
- Incremental models update only new data

## Dependencies
- Requires Alpha Vantage API data
- Assumes daily price data is available
- Expects market index data for beta calculations 