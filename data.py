import asyncio
from ib_insync import *
import pandas as pd
import time
import pandas_ta as ta
# Connect to IB Gateway or TWS application
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)

# Define the stocks
stocks = ['AMZN','NVDA','TSLA','AAPL','AMD','MSFT','PLTR','META','ADBE','SOUN','PFE','SMCI','INTC','MARA','HYG','SLV','QQQ','SPY','IWM']

# Define the timeframes correctly
# timeframes = ['1 min', '5 mins', '15 mins', '1 hour', '1 day']
timeframes = ['1 day','1 hour','15 mins','5 mins','1 min']

async def fetch_and_compute_pivot_points(symbol, timeframe):
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime='',
            durationStr='2 D',
            barSizeSetting=timeframe,
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1
        )
        df = util.df(bars)
        print(df)
        
        if df is None or not all(col in df.columns for col in ['high', 'low', 'close', 'volume']):
            print(f"Error: DataFrame for {symbol} is None or does not have the expected columns.")
            return None
        
        df.columns = df.columns.str.upper()
        
        # Regular pivot points
        df['Pivot'] = (df['HIGH'] + df['LOW'] + df['CLOSE']) / 3
        df['R1'] = 2 * df['Pivot'] - df['LOW']
        df['S1'] = 2 * df['Pivot'] - df['HIGH']
        df['R2'] = df['Pivot'] + (df['HIGH'] - df['LOW'])
        df['S2'] = df['Pivot'] - (df['HIGH'] - df['LOW'])
        df['R3'] = df['Pivot'] + 2 * (df['HIGH'] - df['LOW'])
        df['S3'] = df['Pivot'] - 2 * (df['HIGH'] - df['LOW'])
        
        # Camarilla pivot points
        df['CamarillaPivot'] = (df['HIGH'].shift(1) + df['LOW'].shift(1) + df['CLOSE'].shift(1)) / 3
        df['CR1'] = 2 * df['CamarillaPivot'] - df['LOW'].shift(1)
        df['CS1'] = 2 * df['CamarillaPivot'] - df['HIGH'].shift(1)
        df['CR2'] = df['CamarillaPivot'] + (df['HIGH'].shift(1) - df['LOW'].shift(1))
        df['CS2'] = df['CamarillaPivot'] - (df['HIGH'].shift(1) - df['LOW'].shift(1))
        df['CR3'] = df['CamarillaPivot'] + 2 * (df['HIGH'].shift(1) - df['LOW'].shift(1))
        df['CS3'] = df['CamarillaPivot'] - 2 * (df['HIGH'].shift(1) - df['LOW'].shift(1))
        
        # VWAP
        df.index = pd.to_datetime(df.index)

        df['VWAP'] = ta.vwap(df['HIGH'], df['LOW'], df['CLOSE'], df['VOLUME'])
        
        return df
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

async def process_stocks_for_timeframe(timeframe):
    tasks = [fetch_and_compute_pivot_points(symbol, timeframe) for symbol in stocks]
    results = await asyncio.gather(*tasks)
    
    for symbol, df in zip(stocks, results):
        if df is not None:
            print(f"Pivot Points for {symbol} on {timeframe}:")
            print("Regular Pivot Points:")
            print(df[['Pivot', 'R1', 'S1', 'R2', 'S2', 'R3', 'S3']].tail())
            print("Camarilla Pivot Points:")
            print(df[['CamarillaPivot', 'CR1', 'CS1', 'CR2', 'CS2', 'CR3', 'CS3']].tail())
            print(df['VWAP'].tail())
        else:
            print(f"No data available for {symbol} on {timeframe}.")

# Run the asynchronous processing for each timeframe sequentially
async def process_all_timeframes():
    for timeframe in timeframes:
        print(f"Processing timeframe: {timeframe}")
        await process_stocks_for_timeframe(timeframe)
        # Sleep for 3 seconds to avoid hitting rate limits
        # await asyncio.sleep(3)

# Run the asynchronous processing
import time
start_time = time.time()
ib.run(process_all_timeframes())
stop_time = time.time()
elapsed_time = stop_time - start_time

print("Elapsed time:", elapsed_time, "seconds")
