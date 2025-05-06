# Quantstats Futures Strategy Analysis

A Python module for analyzing futures trading performance using the QuantStats library.

## Overview

This tool generates comprehensive performance reports for futures trading strategies using trade data from CSV files. It calculates key metrics like:

- Total Return & CAGR
- Sharpe, Sortino, and Calmar ratios
- Drawdowns and volatility
- Win rates and profit factors
- Monthly/yearly performance breakdowns

## Requirements

- Python >= 3.8
- pandas >= 2.2.0
- quantstats_lumi >= 0.0.5
- matplotlib >= 3.0.0

## Installation

```bash
pip install -r requirements.txt
```

## Input CSV Format

The input CSV file must contain the following columns:
- entry_time: Datetime when the trade was entered
- exit_time: Datetime when the trade was exited
- pnl: Float representing profit/loss in points

Optional columns that enhance the analysis:
- position: String indicating 'long' or 'short'
- duration: Float indicating trade duration (typically in hours)
- drawdown: Float showing maximum drawdown during trade

## Usage

```python
from quant_analysis import generate_quantstats_report

# Generate a report for NQ futures trades
generate_quantstats_report(
    csv_file="/path/to/trades.csv",
    output_file="Strategy_Report.html",
    instrument_name="Futures",
    strategy_name="Trading Strategy",
    point_value=20,
    initial_capital=100000,
)
```

## Function Parameters

- **csv_file**: Path to CSV file containing trade data
- **output_file**: Path where HTML report will be saved (default: "Strategy_Report.html")
- **instrument_name**: Name of the traded instrument (default: "Futures")
- **strategy_name**: Name of the trading strategy (default: "Trading Strategy")
- **point_value**: Dollar value per point/tick (default: 20.0)
- **initial_capital**: Initial capital in dollars (default: 100000.0)
- **custom_parameters**: Additional parameters to include in the report (default: None)

## Example Output

The generated HTML report includes:
- Strategy parameters and basic statistics
- Equity curve and drawdown analysis
- Monthly and yearly returns
- Risk/reward metrics
- Win/loss statistics

## License

[Apache License 2.0](LICENSE.txt)
