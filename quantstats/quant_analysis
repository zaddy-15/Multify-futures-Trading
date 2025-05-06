import pandas as pd
import quantstats.reports as reports
import os
import warnings
from datetime import datetime
from typing import Dict, Any, Optional, Union


def generate_quantstats_report(
    csv_file: str,
    output_file: str = "Strategy_Report.html",
    instrument_name: str = "Futures",
    strategy_name: str = "Trading Strategy",
    point_value: float = 20.0,
    initial_capital: float = 100000.0,
    timeframe: str = "1-Hour",
    quiet: bool = False,
    custom_parameters: Optional[Dict[str, Any]] = None
) -> str:
    """
    Generate a QuantStats performance report from futures trading data.
    
    Args:
        csv_file: Path to CSV file containing trade data
        output_file: Path where HTML report will be saved
        instrument_name: Name of the traded instrument (e.g., "NQ Futures")
        strategy_name: Name of the trading strategy
        point_value: Dollar value per point/tick (e.g., $20 for NQ)
        initial_capital: Initial capital in dollars
        timeframe: Timeframe of the strategy (e.g., "1-Hour")
        quiet: If True, suppress informational output
        custom_parameters: Additional parameters to include in the report
        
    Returns:
        Path to the generated HTML report
        
    Raises:
        FileNotFoundError: If CSV file cannot be found
        ValueError: If required columns are missing from CSV
    """
    # Suppress warnings
    warnings.filterwarnings("ignore")
    
    # Read the CSV file with trade data
    try:
        trades_df = pd.read_csv(csv_file)
    except FileNotFoundError:
        raise FileNotFoundError(f"CSV file not found: {csv_file}")
    
    # Check for required columns
    required_columns = ['entry_time', 'exit_time', 'position', 'pnl']
    missing_columns = [col for col in required_columns if col not in trades_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in CSV: {', '.join(missing_columns)}")
    
    if not quiet:
        print(f"Analyzing {len(trades_df)} trades...")
    
    # Convert entry and exit times to datetime
    trades_df['entry_time'] = pd.to_datetime(trades_df['entry_time'])
    trades_df['exit_time'] = pd.to_datetime(trades_df['exit_time'])
    
    if not quiet:
        print("\nDate ranges:")
        print(f"Earliest entry: {trades_df['entry_time'].min()}")
        print(f"Latest exit: {trades_df['exit_time'].max()}")
    
    # Sort trades by exit time
    trades_df = trades_df.sort_values('exit_time')
    
    # Create equity curve
    equity = [initial_capital]
    dates = [trades_df['entry_time'].min().date() - pd.Timedelta(days=1)]  # Start day before first trade
    
    # Add each trade's P&L to equity
    for _, trade in trades_df.iterrows():
        trade_dollars = trade['pnl'] * point_value
        equity.append(equity[-1] + trade_dollars)
        dates.append(trade['exit_time'].date())
    
    # Create equity Series
    equity_series = pd.Series(equity[1:], index=pd.DatetimeIndex(dates[1:]))
    
    if not quiet:
        print("\nEquity series:")
        print(f"Start: {equity[0]}")
        print(f"End: {equity[-1]}")
        print(f"Change: {equity[-1] - equity[0]} (${(equity[-1] - equity[0]):,.2f})")
    
    # Calculate returns from equity
    rets = equity_series.pct_change().dropna()
    
    # Add starting point for first day return (from initial capital to first equity point)
    first_day_return = (equity_series.iloc[0] - initial_capital) / initial_capital
    first_day = pd.Series([first_day_return], index=[equity_series.index[0]])
    rets = pd.concat([first_day, rets])
    
    if not quiet:
        print("\nReturns check:")
        print(f"Number of return entries: {len(rets)}")
        print(f"Sum of returns: {rets.sum():.6f}")
        print(f"Non-zero returns: {(rets != 0).sum()}")
        print(f"Min return: {rets.min():.6f}, Max return: {rets.max():.6f}")
    
    # Create strategy parameters
    strategy_parameters = {
        "strategy_name": f"{strategy_name}",
        "instrument": instrument_name,
        "timeframe": timeframe,
        "initial_capital": f"${initial_capital:,.2f}",
        "final_capital": f"${equity[-1]:,.2f}",
        "total_return": f"{(equity[-1] - initial_capital) / initial_capital * 100:.2f}%",
        "point_value": f"${point_value:.2f} per point",
        "trading_period": f"{dates[1]} to {dates[-1]}",
        "total_trades": len(trades_df),
        "win_rate": f"{len(trades_df[trades_df['pnl'] > 0]) / len(trades_df) * 100:.2f}%",
        "total_pnl": f"{trades_df['pnl'].sum():.2f} points (${trades_df['pnl'].sum() * point_value:.2f})",
    }
    
    # Add position breakdown if available
    if 'position' in trades_df.columns:
        long_trades = len(trades_df[trades_df['position'] == 'long'])
        short_trades = len(trades_df[trades_df['position'] == 'short'])
        strategy_parameters.update({
            "long_trades": long_trades,
            "short_trades": short_trades,
            "long_win_rate": f"{len(trades_df[(trades_df['position'] == 'long') & (trades_df['pnl'] > 0)]) / max(long_trades, 1) * 100:.2f}%",
            "short_win_rate": f"{len(trades_df[(trades_df['position'] == 'short') & (trades_df['pnl'] > 0)]) / max(short_trades, 1) * 100:.2f}%"
        })
    
    # Add duration if available
    if 'duration' in trades_df.columns:
        strategy_parameters["avg_trade_duration"] = f"{trades_df['duration'].mean():.2f} hours"
    
    # Add drawdown if available
    if 'drawdown' in trades_df.columns:
        strategy_parameters["max_drawdown_points"] = f"{abs(trades_df['drawdown'].min()):.2f} points"
    
    # Add custom parameters if provided
    if custom_parameters:
        strategy_parameters.update(custom_parameters)
    
    # Generate the HTML report
    html_result = reports.html(
        rets, 
        output=output_file, 
        parameters=strategy_parameters,
        title=f"{instrument_name} {strategy_name} Analysis",
        periods_per_year=252  # Trading days in a year for proper annualization
    )
    
    if not quiet:
        print(f"\nReport generated: {output_file}")
    
    # Browser opening removed as requested
    
    return output_file
