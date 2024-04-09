import asyncio
import logging
from ib_insync import *
import pandas as pd
import csv
from datetime import datetime

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=31)
async def cancel_all_open_orders():
    logging.info("Cancelling all open orders...")
    for trade in ib.openTrades():
        ib.cancelOrder(trade.order)
    logging.info("All open orders cancelled.")

async def close_open_positions():
    possy = await ib.reqPositionsAsync()
    print(possy)
    for pos in possy :
        contract = pos.contract
        if contract.secType == 'STK' :
            if pos.position > 0 :
                action = 'SELL'
                sqoff = Stock(pos.contract.symbol,"SMART",pos.contract.currency)
                await ib.qualifyContractsAsync(sqoff)
            elif pos.position < 0 :
                action = 'BUY'
                sqoff = Stock(pos.contract.symbol,"SMART",pos.contract.currency)
                await ib.qualifyContractsAsync(sqoff)
        elif contract.secType == 'OPT':
            if pos.position > 0 :
                action = 'SELL'
                sqoff = Option(pos.contract.symbol,pos.contract.lastTradeDateOrContractMonth,pos.contract.strike,pos.contract.right,"SMART",pos.contract.multiplier,pos.contract.currency)
                await ib.qualifyContractsAsync(sqoff)
            elif pos.position < 0 :
                action = 'BUY'
                sqoff = Option(pos.contract.symbol,pos.contract.lastTradeDateOrContractMonth,pos.contract.strike,pos.contract.right,"SMART",pos.contract.multiplier,pos.contract.currency)
                await ib.qualifyContractsAsync(sqoff)
        if pos.position != 0 :        
            Order = MarketOrder(action, abs(pos.position))
            ib.placeOrder(sqoff, Order)
        else :
            print("No positions")
            return    

# async def get_spx_value():
#     logging.info("Getting SPX value...")
#     # spx = Index('SPX', 'CBOE')
#     # logging.info(spx)
#     # # ib.qualifyContracts(spx)
#     # # ib.sleep(2)
#     # ib.reqMarketDataType(4)
#     # ticker = ib.reqMktData(spx)
#     # logging.info(ticker)
#     # print(ticker)
#     # market_price = ticker.marketPrice()
#     # print(market_price)
#     # logging.info(market_price)
#     # return market_price
#     spx = Index('SPX', 'CBOE')
#     await ib.qualifyContractsAsync(spx)
#     cds = await ib.reqContractDetailsAsync(spx)
#     ib.reqMarketDataType(1)
#     [ticker] = await ib.reqTickersAsync(spx)
#     spxValue = ticker.marketPrice()
#     print(spxValue)
        
async def get_spx_value():
    # logging.info("Getting SPX value...")
    spx = Index('SPX', 'CBOE')
    # logging.info("check1")
    # # await ib.qualifyContractsAsync(spx)
    # # cds = await ib.reqContractDetails(spx)
    # logging.info("Check2")
    # ib.reqMarketDataType(4)
    # logging.info("Check3")
    # [ticker] = await ib.reqTickersAsync(spx)
    # ib.sleep(1)
    # asyncio.sleep(1)
    # logging.info("Check4")
    # spxValue = ticker.marketPrice()
    # return spxValue 
    ib.reqMarketDataType(1)
    [ticker] = await ib.reqTickersAsync(spx)
    spxValue = ticker.marketPrice()
    print(spxValue)
    # print(ticker)
    # Wait for the market price to be available
# Sleep for 10 ms

    return ticker.marketPrice()


async def get_option_chains( ):
    spx = Index('SPX', 'CBOE')
    await ib.qualifyContractsAsync(spx)
    logging.info("Getting option chains...")
    chains = await ib.reqSecDefOptParamsAsync(spx.symbol, '', spx.secType, spx.conId)
    util.df(chains)
    chain = next(c for c in chains if c.tradingClass == 'SPXW' and c.exchange == 'SMART')
    # Further processing of chains...
    logging.info("Option chains retrieved.")
    return chain

async def process_options(chain,spxValue) :
    strikes = [strike for strike in chain.strikes
        if strike % 5 == 0
        and spxValue - 100 < strike < spxValue + 100]
    expirations = sorted(exp for exp in chain.expirations)[:3]
    rights = ['P', 'C']
    desired_expiration = expirations[0]  # Change this to the desired expiration date
    contracts = [Option('SPX', desired_expiration, strike, right, 'SMART')
             for right in rights
             for strike in strikes]
    contracts = await ib.qualifyContractsAsync(*contracts)
    return contracts

async def choose_strikes(contracts):
    # await ib.qualifyContractsAsync(contracts)
    tickers =  await (ib.reqTickersAsync(*contracts))
    print(tickers)
    call_deltas = [0.05, 0.1]
    put_deltas = [-0.05, -0.1]
    await asyncio.sleep(2)
    call_tickers = [ticker for ticker in tickers if ticker.contract.right == 'C' and ticker.modelGreeks and  call_deltas[0] < ticker.modelGreeks.delta < call_deltas[1]]
    put_tickers = [ticker for ticker in tickers if ticker.contract.right == 'P' and ticker.modelGreeks and  put_deltas[0] > ticker.modelGreeks.delta > put_deltas[1]]
    max_ltp_call = max(call_tickers, key=lambda ticker: ticker.last, default=None)
    max_ltp_put = max(put_tickers, key=lambda ticker: ticker.last, default=None)
    if max_ltp_call and max_ltp_put :
        return max_ltp_call,max_ltp_put
    return None

async def qualify_contracts(max_ltp_put,max_ltp_call):
    short_contracts = [max_ltp_put.contract, max_ltp_call.contract]
    await ib.qualifyContractsAsync(*short_contracts)
    return max_ltp_put.contract,max_ltp_call.contract

async def create_and_qualify_contract(short_contract, strike_offset, right):
    long_contract = Contract()
    long_contract.symbol = short_contract.symbol
    long_contract.secType = short_contract.secType
    long_contract.lastTradeDateOrContractMonth = short_contract.lastTradeDateOrContractMonth
    long_contract.strike = short_contract.strike - strike_offset
    long_contract.right = right
    long_contract.exchange = short_contract.exchange
    await ib.qualifyContractsAsync(long_contract)
    return long_contract

def get_contract_data(contract):
    md = ib.reqTickers(contract)[0]
    greeks = md.modelGreeks
    return {'Timestamp': datetime.now(), 'Contract': contract.localSymbol, 'Strike': contract.strike,
            'Delta': greeks.delta, 'Vega': greeks.vega, 'Gamma': greeks.gamma, 'Theta': greeks.theta, 'IV': greeks.impliedVol}

async def write_csv_greeks(contracts_data):
    csv_file_path = 'greeks_data.csv'
    with open(csv_file_path, 'a', newline='') as csv_file:
        fieldnames = ['Timestamp', 'Contract', 'Strike', 'Delta', 'Vega', 'Gamma', 'Theta', 'IV']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(contracts_data)


async def create_contract(short_contract, long_contract, action):
    return Contract(
        secType='BAG',
        symbol='SPX',
        exchange='SMART',
        currency='USD',
        comboLegs=[
            ComboLeg(
                conId=short_contract.conId,
                ratio=1,
                action='SELL',
                exchange='SMART'),
            ComboLeg(
                conId=long_contract.conId,
                ratio=1,
                action=action,
                exchange='SMART'),
        ])


async def place_order(contract, order_ref):
    order = MarketOrder(action='BUY', totalQuantity=1, orderRef=order_ref)
    return ib.placeOrder(contract, order)
async def get_fills(instrument):
    while True:
        avg_fill_price = instrument.orderStatus.avgFillPrice
        if avg_fill_price != 0.0:
            return avg_fill_price
        else:
            await asyncio.sleep(1)  

async def monitor(bear_call, bull_put, terminate_calls, terminate_puts, sqoff_calls, sqoff_puts, bull_fill, bear_fill):
    # print("Bull fill",bull_fill)
    # print("bear fill",bear_fill)
    # call_flag = False
    # put_flag = False
    # flag = None
    # while not (call_flag and put_flag):
    # # if flag == None :
    #     # ltp_call =  ib.reqMktData(bear_call, '', False, False).marketPrice()
    #     # ltp_put = ib.reqMktData(bull_put, '', False, False).marketPrice()
    #     ib.reqMarketDataType(1)
    #     [ticker] = await ib.reqTickersAsync(bear_call)
    #     ltp_call = ticker.marketPrice()
    #     [ticker] = await ib.reqTickersAsync(bull_put)
    #     ltp_put = ticker.marketPrice()
    #     print(abs(ltp_call+ltp_put)," Current premia")
    #     # ib.sleep(1)

    #     if ltp_call == -0.05 and not call_flag:
    #         ct = ib.placeOrder(bear_call, terminate_calls)
    #         # call_flag = True

    #     if ltp_put == -0.05 and not put_flag:
    #         cp = ib.placeOrder(bull_put, terminate_puts)
    #         # put_flag = True

    #     if bull_fill*2 - 0.05 > ltp_call and not call_flag:
    #         print("Call side SL hit")
    #         ct = ib.placeOrder(bear_call, sqoff_calls)
    #         # call_flag = True
    #         await asyncio.sleep(1)

    #     if bear_fill*2 - 0.05 > ltp_put and not put_flag:
    #         cp = ib.placeOrder(bull_put, sqoff_puts)
    #         print("Put side SL hit")
    #         # put_flag = True
    #         await asyncio.sleep(0.5)
    # await asyncio.sleep(1)
    # ce = await get_fills(ct)
    # pe = await get_fills(cp)
    # return ce,pe
    contracts = [bear_call,bull_put]
    bc = contracts[0]
    bp = contracts[1]
    await ib.qualifyContractsAsync(*contracts)
    for contract in contracts:
        ib.reqMktData(contract, '', False, False)
    call = ib.ticker(bc)
    put = ib.ticker(bp)
    await asyncio.sleep(2)
    call_flag = False
    put_flag = False
    while not (call_flag and put_flag):
        if call_flag == False and put_flag == False :
            print(f"Net Premia is {abs(call.marketPrice()+put.marketPrice())}")
        elif call_flag == False :
            print(f"Net Premia is {call.marketPrice()}")
        elif put_flag == False :
            print(f"Net Premia is {put.marketPrice()}")
        if call.marketPrice() == -0.05 and not call_flag:
            ct = ib.placeOrder(bear_call, terminate_calls)
            call_flag = True
            ib.cancelMktData(call)
        if put.marketPrice() == -0.05 and not put_flag:
            ct = ib.placeOrder(bull_put, terminate_puts)
            put_flag = True
            ib.cancelMktData(put)

        if bull_fill*2 - 0.05 > call.marketPrice() and not call_flag:
            print("Call side SL hit")
            ct = ib.placeOrder(bear_call, sqoff_calls)
            call_flag = True
            ib.cancelMktData(call)
            await asyncio.sleep(1)

        if bear_fill*2 - 0.05 > put.marketPrice() and not put_flag:
            cp = ib.placeOrder(bull_put, sqoff_puts)
            print("Put side SL hit")
            put_flag = True
            ib.cancelMktData(put)
            await asyncio.sleep(1)
        await asyncio.sleep(1)  
    print("Loop broken")    


async def log_to_csv(contract_names, prices, action):
    if len(contract_names) != len(prices):
        print("Error: contract_names and prices lists must be of the same length.")
        return

    filename='log.csv'
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for contract_name, price in zip(contract_names, prices):
            writer.writerow([timestamp, contract_name, price, action])


# async def main():
logging.info("Starting script...")

ib.run(cancel_all_open_orders())
ib.run(close_open_positions())
spxValue =  ib.run(get_spx_value())
chain = ib.run(get_option_chains())
# print(chain)
print(len(chain))
#fails sometimes so fix this
options =  ib.run(process_options(chain,spxValue))
print(options)
#This fails too so fix this
call,put =  ib.run(choose_strikes(options))
print(f"{call.contract.strike} is short leg\n")
print(f"{put.contract.strike} is short put leg\n")
logging.info("Short strikes selected")
short_put,short_call =  ib.run(qualify_contracts(call,put))
offset = 30
long_call = ib.run(create_and_qualify_contract(short_call,offset,short_call.right))
long_put = ib.run(create_and_qualify_contract(short_put,-offset,short_put.right))
ib.run( ib.qualifyContractsAsync(long_call,long_put))
print("All strikes qualified")
contracts = [short_put, short_call, long_put, long_call]
contracts_data = ([get_contract_data(contract) for contract in contracts])
print(contracts_data)
ib.run(write_csv_greeks(contracts_data))
print("Greeks Logged")
bear_call = ib.run(create_contract(short_call, long_call, 'BUY'))
bull_put = ib.run( create_contract(short_put, long_put, 'BUY'))
trade_call = ib.run(place_order(bear_call, 'BearCall'))
trade_put = ib.run(place_order(bull_put, 'BullPut'))
ib.sleep(4)
print("Orders placed.")
logging.info("Orders placed")
bc_avg =  ib.run(get_fills(trade_call))
bp_avg = ib.run(get_fills(trade_put))
ib.run( log_to_csv([bear_call,bull_put],[bc_avg,bp_avg],"Entry"))
sqoff_calls = LimitOrder(action = 'SELL',totalQuantity = 1,orderRef='Bear call close',lmtPrice = bc_avg - 0.15)
sqoff_puts = LimitOrder(action = 'SELL',totalQuantity = 1,orderRef='Bull put close',lmtPrice = bp_avg - 0.15)
terminate_calls = MarketOrder(action = 'SELL',totalQuantity = 1,orderRef='Bear call terminate')
terminate_puts = MarketOrder(action = 'SELL',totalQuantity = 1,orderRef='Bull put terminate')    
net_premia = bc_avg + bp_avg
print(f"{net_premia} is the net premium received")
ce,pe = ib.run( monitor(bear_call,bull_put,terminate_calls,terminate_puts,sqoff_calls,sqoff_puts,bc_avg,bp_avg))
print("Trade complete")
ib.run( write_csv_greeks(contracts_data))
ib.run(log_to_csv([bear_call,bull_put],[ce,pe],"Exit"))

logging.info("Script completed.")


# if __name__ == "__main__":
#     asyncio.run(main())
