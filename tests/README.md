# Data Quality Tests

## Freshness Tests
- `assert_data_freshness.sql`: Ensures data is updated by 9am EST
- Checks last trading date against current date
- Accounts for weekends and holidays

## Continuity Tests
- `assert_trading_dates_continuous.sql`: Checks for missing trading days
- Allows for weekends (2 days) and holidays (1 day)
- Flags unexpected gaps in trading data

## Value Range Tests
- Tests for acceptable ranges in key metrics
- RSI: 0-100
- Beta: typically 0-3
- Returns: reasonable ranges (-50% to +50% daily) 