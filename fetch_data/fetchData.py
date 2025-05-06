import sys
import os
import pandas as pd
import datetime
import psycopg2
import psycopg2.extras as psycopg2_e

parent_dir = os.path.join(os.path.dirname(__file__), '../')
normalized_path = os.path.normpath(parent_dir)

config_dir = os.path.join(normalized_path, 'utils')
sys.path.append(config_dir)

from configReader import ConfigReader 
config_reader = ConfigReader('config.ini')
conn = psycopg2.connect(config_reader.get_db_params())
cursor = conn.cursor(cursor_factory=psycopg2_e.RealDictCursor)

def fetchTradingDays():
    """
    Function to fetch the Trading Days of SPX market.

    Parameters
    ----------
    None

    Returns
    -------
    pd.Dataframe 
    """
    global ip
    global conn
    global cursor
    query_ = f"""
                SELECT DISTINCT DATE_TRUNC('day', Datetime) AS dates
                FROM spx_index 
            """
    try:
        cursor.execute(query_)
    except Exception as e:
        try:
            cursor.close()
        except:
            conn.close()
            conn = psycopg2.connect(config_reader.get_db_params())
                    
        cursor = conn.cursor(cursor_factory=psycopg2_e.RealDictCursor)
        cursor.execute(query_)

    df = pd.DataFrame(cursor.fetchall())
    df['dates'] = pd.to_datetime(df['dates']).dt.date
    return df

def fetchExpiryDays(symbol:str):
    """
    Function to get the Expiry dates of a particular symbol.
    
    Parameters
    ----------
    symbol:str - SPXW

    Returns
    -------
    pd.Dataframe 
    """
    global ip
    global conn
    global cursor
    
    if(symbol not in ['SPXW']):
        raise Exception('Invalid symbol')
    query_ = f"""
                SELECT DISTINCT Expiry
                FROM spxw_options_ohlcv
                ORDER BY Expiry ASC;
            """
    try:
        cursor.execute(query_)
    except Exception as e:
        try:
            cursor.close()
        except:
            conn.close()
            conn = psycopg2.connect(config_reader.get_db_params())
                    
        cursor = conn.cursor(cursor_factory=psycopg2_e.RealDictCursor)
        cursor.execute(query_)

    df = pd.DataFrame(cursor.fetchall())
    df['dates'] = pd.to_datetime(df['Expiry'], format='%Y-%m-%d').dt.date
    df = df.sort_values(by='dates')
    df = df[['dates']]
    df = df.reset_index(drop=True)
    return df

def fetchDataIndex(symbol:str,startDate:datetime.date,endDate:datetime.date):
    """
    Get the Spot data for the symbol requested.
    
    Parameters
    ----------
    symbol:str - SPX
    startDate:datetime.date       
    endDate:datetime.date

    Returns
    -------
    pd.Dataframe 
    """
    global ip
    global conn
    global cursor
    
    if(symbol not in ['SPX']):
        raise Exception('Invalid symbol')
        
    if isinstance(startDate, datetime.date):
        startDate = startDate.strftime('%Y-%m-%d')
    
    if isinstance(endDate, datetime.date):
        endDate = endDate.strftime('%Y-%m-%d')
        
    endDate = datetime.datetime.strptime(endDate, '%Y-%m-%d')
    endDate = endDate + datetime.timedelta(days=1)
    endDate = endDate.strftime('%Y-%m-%d')
    
    query_ = f"""
            SELECT Datetime as timestamp, Open, High, Low, Close from {symbol}_index WHERE Datetime >= '{startDate}' AND Datetime <= '{endDate}' ORDER BY Datetime
            """
    try:
        cursor.execute(query_)
    except Exception as e:
        try:
            cursor.close()
        except:
            conn.close()
            conn = psycopg2.connect(config_reader.get_db_params())
                    
        cursor = conn.cursor(cursor_factory=psycopg2_e.RealDictCursor)
        cursor.execute(query_)

    df = pd.DataFrame(cursor.fetchall())
    if(df.empty or len(df) == 0):
        raise Exception(f"Index data not found - {startDate}__{endDate}__{symbol}")
    
    df = df.set_index("timestamp")
    df.index = pd.to_datetime(df.index, format="%Y-%m-%d %H:%M:%S")
    return df

def fetchDataOptions(symbol:str,startDate:datetime.date,endDate:datetime.date,strike,expiryDate,callPut):
    """
    Get the Options data for the symbol, strike, expiry,callPut requested.
    
    Parameters
    ----------
    symbol:str - SPXW
    startDate :datetime.date       
    endDate: datetime.date    
    strike:
    expiryDate: datetime.date
    callPut: CE or PE

    Returns
    -------
    pd.Dataframe 
    """
    global ip
    global conn
    global cursor
    print('Fetching Option Data for:', symbol , 'from:', startDate, 'to:', endDate, 'for expiry:', expiryDate)
    if(symbol not in ['SPXW']):
        raise Exception('Invalid symbol')

    if isinstance(startDate, datetime.date):
        startDate = startDate.strftime('%Y-%m-%d')
    if isinstance(endDate, datetime.date):
        endDate = endDate.strftime('%Y-%m-%d')
    if isinstance(expiryDate, datetime.date):
        expiryDate = expiryDate.strftime('%Y-%m-%d')

    endDate = datetime.datetime.strptime(endDate, '%Y-%m-%d')
    endDate = endDate + datetime.timedelta(days=1)
    endDate = endDate.strftime('%Y-%m-%d')

    query_ = f"""
            with 
            meta_query  as (select  Datetime as timestamp,OptionType,Strike,Expiry,Open,High,Low,Close,Volume,Datetime from 'spxw_options_ohlcv' where Datetime > '{startDate}' and Datetime < '{endDate}')
            SELECT min(timestamp) as timestamp, OptionType,Strike,Expiry,first(Open) as Open, max(High) as High, min(Low) as Low, last(Close) as Close, sum(Volume) as Volume from meta_query where timestamp IN '2000-01-01T09:15;404m;1d;10000' and  (Strike = {strike}) and (Expiry = '{expiryDate}') and (OptionType='{callPut}') SAMPLE BY 1m; 
            """
    try:
        cursor.execute(query_)
    except Exception as e:
        try:
            cursor.close()
        except:
            conn.close()
            conn = psycopg2.connect(config_reader.get_db_params())
                    
        cursor = conn.cursor(cursor_factory=psycopg2_e.RealDictCursor)
        cursor.execute(query_)

    df = pd.DataFrame(cursor.fetchall())
    if(df.empty or len(df) == 0):
        raise Exception(f"\nOptions data Not found - {startDate}__{endDate}__{strike}__{expiryDate}__{callPut}")
    else:
        df.timestamp = pd.to_datetime(df.timestamp, format="%Y-%m-%d %H:%M:%S")
        df = df.set_index("timestamp")
        df = df.between_time('09:30', '15:59')
        df.index = df.index.round('s')
    return df

def fetchContractMonthFutures(symbol:str):
    global ip
    global conn
    global cursor
    
    query_ = f"""
             SELECT DISTINCT Contract_month
                FROM {symbol}_futures
                ORDER BY Contract_month ASC;
            """
    try:
        cursor.execute(query_)
    except Exception as e:
        try:
            cursor.close()
        except:
            conn.close()
            conn = psycopg2.connect(config_reader.get_db_params())
                    
        cursor = conn.cursor(cursor_factory=psycopg2_e.RealDictCursor)
        cursor.execute(query_)

    df = pd.DataFrame(cursor.fetchall())
    return df

def fetchDataFutures(symbol:str,startDate:datetime.date,endDate:datetime.date, contractMonth):
    global ip
    global conn
    global cursor
  
    if isinstance(startDate, datetime.date):
        startDate = startDate.strftime('%Y-%m-%d')
    
    if isinstance(endDate, datetime.date):
        endDate = endDate.strftime('%Y-%m-%d')
        
    endDate = datetime.datetime.strptime(endDate, '%Y-%m-%d')
    endDate = endDate + datetime.timedelta(days=1)
    endDate = endDate.strftime('%Y-%m-%d')
    
    query_ = f"""
            SELECT Datetime as timestamp, Open, High, Low, Close, Volume from {symbol}_futures WHERE Datetime >= '{startDate}' AND Datetime <= '{endDate}' AND Contract_month='{contractMonth}' ORDER BY Datetime
            """
    try:
        cursor.execute(query_)
    except Exception as e:
        try:
            cursor.close()
        except:
            conn.close()
            conn = psycopg2.connect(config_reader.get_db_params())
                    
        cursor = conn.cursor(cursor_factory=psycopg2_e.RealDictCursor)
        cursor.execute(query_)

    df = pd.DataFrame(cursor.fetchall())
    if(df.empty or len(df) == 0):
        raise Exception(f"Futures data not found - {startDate}__{endDate}__{symbol}")
    
    df = df.set_index("timestamp")
    df.index = pd.to_datetime(df.index, format="%Y-%m-%d %H:%M:%S")
    return df

if __name__=="__main__":
    print("fetchData.py (main method) is running")
   # print("Trading Days SPX:", '\n', fetchTradingDays())
    #print("Expiry Days for SPXW", '\n', fetchExpiryDays('SPXW'))
    print("Options Data ",'\n',fetchDataOptions('SPXW','2024-05-31','2024-05-31',4235,'2024-05-31','CE'))
    print("Index Data ", '\n', fetchDataIndex('SPX', datetime.date(2025, 2, 4), datetime.date(2025, 2, 5)))











#### ARCHIVE #####
# def fetchDataFutures(symbol:str, startDate:datetime.date, endDate:datetime.date):
#     """
#     Get the Futures data for the symbol requested.
    
#     Parameters
#     ----------
#     symbol:str - NIFTY , BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX, BANKEX    
#     startDate :datetime.date       
#     endDate: datetime.date    

#     Returns
#     -------
#     pd.Dataframe 
#     """
#     global ip
#     global conn
#     global cursor

#     if symbol not in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX']:
#         raise Exception('Invalid symbol')

#     if isinstance(startDate, datetime.date):
#         startDate = startDate.strftime('%Y-%m-%d')
#     if isinstance(endDate, datetime.date):
#         endDate = endDate.strftime('%Y-%m-%d')
  
#     endDate = datetime.datetime.strptime(endDate, '%Y-%m-%d')
#     endDate = endDate + datetime.timedelta(days=1)
#     endDate = endDate.strftime('%Y-%m-%d')

#     query_ = f"""
#         SELECT
#             Datetime AS timestamp, Open, High, Low, Close, Volume, Expiry
#         FROM
#             FUTURES_NSE
#         WHERE
#             Datetime >= '{startDate}'
#             AND Datetime < '{endDate}'
#             AND Symbol = '{symbol}'
#         """

#     try:
#         cursor.execute(query_)
#     except Exception as e:
#         try:
#             cursor.close()
#         except:
#             conn.close()
#             conn = psycopg2.connect(config_reader.get_db_params())
                    
#         cursor = conn.cursor(cursor_factory=psycopg2_e.RealDictCursor)
#         cursor.execute(query_)

#     df = pd.DataFrame(cursor.fetchall())

#     if(df.empty or len(df) <= 0):
#         raise Exception(f"Futures data Not found - {startDate}__{endDate}")
#     else:
#         df.timestamp = pd.to_datetime(df.timestamp, format="%Y-%m-%d %H:%M:%S")
#         df = df.set_index("timestamp")
#         df = df.between_time('09:15', '15:30')
#         df.index = df.index.round('s')
#     return df


# def fetchDataEODFutures(symbol:str, startDate:datetime.date, endDate:datetime.date):
#     """
#     Get the EOD Futures data for the symbol requested.
    
#     Parameters
#     ----------
#     symbol:str - NIFTY , BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX, BANKEX    
#     startDate :datetime.date       
#     endDate: datetime.date    

#     Returns
#     -------
#     pd.Dataframe 
#     """
#     global ip
#     global conn
#     global cursor

#     if symbol not in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX']:
#         raise Exception('Invalid symbol')

#     if isinstance(startDate, datetime.date):
#         startDate = startDate.strftime('%Y-%m-%d')
#     if isinstance(endDate, datetime.date):
#         endDate = endDate.strftime('%Y-%m-%d')

#     endDate = datetime.datetime.strptime(endDate, '%Y-%m-%d')
#     endDate = endDate + datetime.timedelta(days=1)
#     endDate = endDate.strftime('%Y-%m-%d')

#     query_ = f"""
#         SELECT
#             Datetime AS timestamp, Open, High, Low, Close, Volume, Expiry
#         FROM
#             NSE_FUTURES_EOD
#         WHERE
#             Datetime > '{startDate}'
#             AND Datetime < '{endDate}'
#             AND Symbol = '{symbol}' 
#     """

#     try:
#         cursor.execute(query_)
#     except Exception as e:
#         try:
#             cursor.close()
#         except:
#             conn.close()
#             conn = psycopg2.connect(config_reader.get_db_params())
                    
#         cursor = conn.cursor(cursor_factory=psycopg2_e.RealDictCursor)
#         cursor.execute(query_)

#     df = pd.DataFrame(cursor.fetchall())

#     if(df.empty or len(df) <= 0):
#         raise Exception(f"Futures data Not found - {startDate}__{endDate}")
#     else:
#         df.timestamp = pd.to_datetime(df.timestamp, format="%Y-%m-%d %H:%M:%S")
#         df = df.set_index("timestamp")
#         df.index = df.index.round('s')
#     return df
