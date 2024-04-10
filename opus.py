import asyncio
from ib_insync import *
import pandas as pd
import time
import pandas_ta as ta
import numpy as np

# Connect to IB Gateway or TWS application
ib = IB()


def reconnect():
    while not ib.isConnected():
        try:
            ib.disconnect()
            ib.connect('127.0.0.1', 7497, clientId=1)
            print("Reconnected to IB server.")
            break
        except ConnectionRefusedError:
            print("Failed to connect to IB server. Retrying in 5 seconds...")
            time.sleep(5)

# Define the stocks
stocks = ['AMZN', 'NVDA', 'TSLA', 'AAPL', 'AMD', 'MSFT', 'PLTR', 'META', 'ADBE', 'SOUN', 'PFE', 'SMCI', 'INTC', 'MARA', 'HYG', 'SLV', 'QQQ', 'SPY', 'IWM']

# Define the timeframes
timeframes = ['1 day', '1 hour', '15 mins', '5 mins', '1 min']

# Define the duration strings for each timeframe
duration_strings = {
    '1 day': '30 D',
    '1 hour': '3 D',
    '15 mins': '1 D',
    '5 mins': '1 D',
    '1 min': '1 D'
}

def half_trend(df, period=14, atr_multiplier=2.0):
    hl2 = (df['HIGH'] + df['LOW']) / 2
    atr = ta.atr(df['HIGH'], df['LOW'], df['CLOSE'], length=period)
    trend = np.zeros(len(df))
    trend_direction = np.zeros(len(df))
    
    for i in range(period, len(df)):
        if i == period:
            if hl2.iloc[i] > hl2.iloc[i - 1]:
                trend[i] = hl2.iloc[i] - atr_multiplier * atr.iloc[i]
                trend_direction[i] = 1
            else:
                trend[i] = hl2.iloc[i] + atr_multiplier * atr.iloc[i]
                trend_direction[i] = -1
        else:
            if trend_direction[i - 1] == 1:
                if hl2.iloc[i] > trend[i - 1]:
                    trend[i] = max(trend[i - 1], hl2.iloc[i] - atr_multiplier * atr.iloc[i])
                    trend_direction[i] = 1
                else:
                    trend[i] = hl2.iloc[i] + atr_multiplier * atr.iloc[i]
                    trend_direction[i] = -1
            else:
                if hl2.iloc[i] < trend[i - 1]:
                    trend[i] = min(trend[i - 1], hl2.iloc[i] + atr_multiplier * atr.iloc[i])
                    trend_direction[i] = -1
                else:
                    trend[i] = hl2.iloc[i] - atr_multiplier * atr.iloc[i]
                    trend_direction[i] = 1
    
    return pd.Series(trend, index=df.index), pd.Series(trend_direction, index=df.index)

async def fetch_and_compute_pivot_points(symbol, timeframe):
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime='',
            durationStr=duration_strings[timeframe],
            barSizeSetting=timeframe,
            whatToShow='TRADES',
            useRTH=True,
                        formatDate=1,
            timeout=60 # Increased timeout value
        )
        df = util.df(bars)

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

        # RSI
        df['RSI'] = ta.rsi(df['CLOSE'], length=14)

        # Half Trend
        df['HALFTREND'], df['TREND_DIRECTION'] = half_trend(df)

        return df

    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None



class IBConnectionError(Exception):
    pass

async def process_stocks_for_timeframe(timeframe):
    tasks = [fetch_and_compute_pivot_points(symbol, timeframe) for symbol in stocks]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for symbol, df in zip(stocks, results):
        if isinstance(df, Exception):
            print(f"Error processing {symbol} on {timeframe}: {df}")
        elif df is not None:
            print(f"\nLatest values for {symbol} on {timeframe}:")
            latest_values = {
                'Close': df['CLOSE'].iloc[-1],
                'Regular Pivot': df['Pivot'].iloc[-1],
                'R1': df['R1'].iloc[-1],
                'S1': df['S1'].iloc[-1],
                'R2': df['R2'].iloc[-1],
                'S2': df['S2'].iloc[-1],
                'R3': df['R3'].iloc[-1],
                'S3': df['S3'].iloc[-1],
                'Camarilla Pivot': df['CamarillaPivot'].iloc[-1],
                'CR1': df['CR1'].iloc[-1],
                'CS1': df['CS1'].iloc[-1],
                'CR2': df['CR2'].iloc[-1],
                'CS2': df['CS2'].iloc[-1],
                'CR3': df['CR3'].iloc[-1],
                'CS3': df['CS3'].iloc[-1],
                'VWAP': df['VWAP'].iloc[-1],
                'RSI': df['RSI'].iloc[-1],
                'Half Trend': df['HALFTREND'].iloc[-1],
                'Trend Direction': df['TREND_DIRECTION'].iloc[-1]
            }
            for key, value in latest_values.items():
                if isinstance(value, float):
                    print(f"{key}: {value:.2f}")
                else:
                    print(f"{key}: {value}")

            try:
                # Request option parameters
                opt_params = await ib.reqContractDetailsAsync(Stock(symbol, 'SMART', 'USD'))
                opt_chain = await ib.reqSecDefOptParamsAsync(opt_params[0].contract, '', False, [])

                expiry_dates = [exp for exp in opt_chain if exp['expirations']]
                if expiry_dates:
                    closest_expiry = min(expiry_dates[0]['expirations'])
                    print(f"Closest expiry date for {symbol}: {closest_expiry}")
                    regular_pivot = df['Pivot'].iloc[-1]
                    r3 = df['R3'].iloc[-1]
                    s3 = df['S3'].iloc[-1]

                    short_call_strike = int(regular_pivot)
                    short_put_strike = int(regular_pivot)
                    long_call_strike = int(r3)
                    long_put_strike = int(s3)

                    # Create the iron condor legs
                    short_call = Option(symbol, closest_expiry, 'C', short_call_strike, 'SMART', tradingClass=opt_params[0].contract.tradingClass)
                    long_call = Option(symbol, closest_expiry, 'C', long_call_strike, 'SMART', tradingClass=opt_params[0].contract.tradingClass)
                    short_put = Option(symbol, closest_expiry, 'P', short_put_strike, 'SMART', tradingClass=opt_params[0].contract.tradingClass)
                    long_put = Option(symbol, closest_expiry, 'P', long_put_strike, 'SMART', tradingClass=opt_params[0].contract.tradingClass)

                    # Create the iron condor combo contract
                    iron_condor = Contract()
                    iron_condor.symbol = symbol
                    iron_condor.secType = 'BAG'
                    iron_condor.currency = 'USD'
                    iron_condor.exchange = 'SMART'
                    iron_condor.comboLegs = [
                        ComboLeg(conId=short_call.conId, ratio=1, action='SELL', exchange='SMART'),
                        ComboLeg(conId=long_call.conId, ratio=1, action='BUY', exchange='SMART'),
                        ComboLeg(conId=short_put.conId, ratio=1, action='SELL', exchange='SMART'),
                        ComboLeg(conId=long_put.conId, ratio=1, action='BUY', exchange='SMART')
                    ]

                    # Place the iron condor order as a market order
                    order = MarketOrder('BUY', 1)
                    trade = ib.placeOrder(iron_condor, order)
                    print(f"Iron Condor order placed for {symbol}: {trade}")
                else:
                    print(f"No option chain found for {symbol}")

            except asyncio.exceptions.CancelledError:
                print(f"Error requesting option parameters for {symbol}. Reconnecting...")
                raise IBConnectionError()
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
                raise IBConnectionError()
        else:
            print(f"No data available for {symbol} on {timeframe}.")


# async def process_stocks_for_timeframe(timeframe):
#     tasks = [fetch_and_compute_pivot_points(symbol, timeframe) for symbol in stocks]
#     results = await asyncio.gather(*tasks, return_exceptions=True)

#     for symbol, df in zip(stocks, results):
#         if isinstance(df, Exception):
#             print(f"Error processing {symbol} on {timeframe}: {df}")
#         elif df is not None:
#             print(f"\nLatest values for {symbol} on {timeframe}:")
#             latest_values = {
#                 'Close': df['CLOSE'].iloc[-1],
#                 'Regular Pivot': df['Pivot'].iloc[-1],
#                 'R1': df['R1'].iloc[-1],
#                 'S1': df['S1'].iloc[-1],
#                 'R2': df['R2'].iloc[-1],
#                 'S2': df['S2'].iloc[-1],
#                 'R3': df['R3'].iloc[-1],
#                 'S3': df['S3'].iloc[-1],
#                 'Camarilla Pivot': df['CamarillaPivot'].iloc[-1],
#                 'CR1': df['CR1'].iloc[-1],
#                 'CS1': df['CS1'].iloc[-1],
#                 'CR2': df['CR2'].iloc[-1],
#                 'CS2': df['CS2'].iloc[-1],
#                 'CR3': df['CR3'].iloc[-1],
#                 'CS3': df['CS3'].iloc[-1],
#                 'VWAP': df['VWAP'].iloc[-1],
#                 'RSI': df['RSI'].iloc[-1],
#                 'Half Trend': df['HALFTREND'].iloc[-1],
#                 'Trend Direction': df['TREND_DIRECTION'].iloc[-1]
#             }
#             for key, value in latest_values.items():
#                 if isinstance(value, float):
#                     print(f"{key}: {value:.2f}")
#                 else:
#                     print(f"{key}: {value}")

#             try:
#                 # Request option parameters
#                 opt_params = await ib.reqContractDetailsAsync(Stock(symbol, 'SMART', 'USD'))
#                 opt_chain = await ib.reqSecDefOptParamsAsync(opt_params[0].contract, '', False, [])

#                 # Find the closest expiry date
#                 expiry_dates = [exp for exp in opt_chain if exp['expirations']]
#                 if expiry_dates:
#                     closest_expiry = min(expiry_dates[0]['expirations'])
#                     print(f"Closest expiry date for {symbol}: {closest_expiry}")
#                     regular_pivot = df['Pivot'].iloc[-1]
#                     r3 = df['R3'].iloc[-1]
#                     s3 = df['S3'].iloc[-1]

#                     short_call_strike = int(regular_pivot)
#                     short_put_strike = int(regular_pivot)
#                     long_call_strike = int(r3)
#                     long_put_strike = int(s3)

#                     # Create the iron condor legs
#                     short_call = Option(symbol, closest_expiry, 'C', short_call_strike, 'SMART', tradingClass=opt_params[0].contract.tradingClass)
#                     long_call = Option(symbol, closest_expiry, 'C', long_call_strike, 'SMART', tradingClass=opt_params[0].contract.tradingClass)
#                     short_put = Option(symbol, closest_expiry, 'P', short_put_strike, 'SMART', tradingClass=opt_params[0].contract.tradingClass)
#                     long_put = Option(symbol, closest_expiry, 'P', long_put_strike, 'SMART', tradingClass=opt_params[0].contract.tradingClass)

#                     # Create the iron condor combo contract
#                     iron_condor = Contract()
#                     iron_condor.symbol = symbol
#                     iron_condor.secType = 'BAG'
#                     iron_condor.currency = 'USD'
#                     iron_condor.exchange = 'SMART'
#                     iron_condor.comboLegs = [
#                         ComboLeg(conId=short_call.conId, ratio=1, action='SELL', exchange='SMART'),
#                         ComboLeg(conId=long_call.conId, ratio=1, action='BUY', exchange='SMART'),
#                         ComboLeg(conId=short_put.conId, ratio=1, action='SELL', exchange='SMART'),
#                         ComboLeg(conId=long_put.conId, ratio=1, action='BUY', exchange='SMART')
#                     ]

#                     # Place the iron condor order as a market order
#                     order = MarketOrder('BUY', 1)
#                     trade = ib.placeOrder(iron_condor, order)
#                     print(f"Iron Condor order placed for {symbol}: {trade}")
#                 else:
#                     print(f"No option chain found for {symbol}")

#             except asyncio.exceptions.CancelledError:
#                 print(f"Error requesting option parameters for {symbol}. Reconnecting...")
#                 reconnect()
#             except Exception as e:
#                 print(f"Error processing {symbol}: {e}")
#                 reconnect()
#         else:
#             print(f"No data available for {symbol} on {timeframe}.")

async def process_all_timeframes():
    for timeframe in timeframes:
        print(f"\nProcessing timeframe: {timeframe}")
        try:
            await process_stocks_for_timeframe(timeframe)
        except IBConnectionError:
            print("Error processing timeframe. Reconnecting...")
            raise
        await asyncio.sleep(5)
        
        
def run_loop():
    while True:
        try:
            ib.run(process_all_timeframes())
            break
        except IBConnectionError:
            print("Error in main loop. Reconnecting...")
            try:
                ib.disconnect()
                ib.connect('127.0.0.1', 7497, clientId=1)
            except (ConnectionRefusedError, asyncio.exceptions.TimeoutError):
                print("Failed to reconnect. Retrying in 5 seconds...")
                time.sleep(5)        
         # Sleep for 5 seconds to avoid hitting rate limits


if __name__ == "__main__":
    ib.connect('127.0.0.1', 7497, clientId=1)
    start_time = time.time()
    run_loop()
    stop_time = time.time()
    elapsed_time = stop_time - start_time
    print("\nElapsed time:", elapsed_time, "seconds")