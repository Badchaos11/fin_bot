import datetime
import threading
import time

from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper

asks = []


class IBapi(EWrapper, EClient):

    def __init__(self):
        EClient.__init__(self, self)

    def realtimeBar(self, reqId: TickerId, time: int, open_: float, high: float, low: float, close: float,
                    volume: int, wap: float, count: int):
        print(f"Realtime Bar (now for Ask).\n"
              f"Req ID: {reqId}, Date: {time};\n"
              f"Open: {open_}, High: {high}, Low: {low}, Close: {close};\n"
              f"Volume: {volume}, WAP: {wap}, Count: {count}")
        global asks
        asks.append(close)
        return close


def contractor(data: list):

    contract = Contract()
    contract.symbol = data[0]
    contract.secType = "OPT"
    contract.exchange = data[2]
    contract.currency = data[1]
    contract.multiplier = data[5]
    contract.right = data[3]
    contract.strike = data[4]
    contract.lastTradeDateOrContractMonth = data[6]
    if data[0] == 'UBI':
        contract.tradingClass = 'US1'
    if data[0] == 'TTE':
        contract.tradingClass = 'TOT4'

    return contract


def contractor_hedges(data: list, right: str):

    contract = Contract()
    contract.symbol = data[0]
    contract.secType = "OPT"
    contract.exchange = data[2]
    contract.currency = data[1]
    contract.multiplier = data[4]
    contract.right = right
    contract.strike = data[3]
    contract.lastTradeDateOrContractMonth = data[5]
    contract.tradingClass = data[7]

    return contract


def ask_checker(ticker: list):
    def run_loop():
        app.run()

    app = IBapi()
    app.connect('127.0.0.1', 7497, 735816)
    print(f'Connected {datetime.datetime.now().timestamp()}')
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    print(f'Started {datetime.datetime.now().timestamp()}')

    time.sleep(1)

    contract = contractor(ticker)
    print(f'Contracted {datetime.datetime.now().timestamp()}')
    x = app.reqRealTimeBars(19001, contract, whatToShow='ASK', useRTH=True, barSize=5, realTimeBarsOptions=[])
    print(f'Ordered {datetime.datetime.now().timestamp()}')
    time.sleep(5)
    app.disconnect()
    print(f'Disconected {datetime.datetime.now().timestamp()}')

    return x


def bid_checker(ticker: list, right: str):
    def run_loop():
        app.run()

    app = IBapi()
    app.connect('127.0.0.1', 7497, 735816)
    print(f'Connected {datetime.datetime.now().timestamp()}')
    time.sleep(2)
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    print(f'Started {datetime.datetime.now().timestamp()}')

    time.sleep(1)

    contract = contractor_hedges(ticker, right)
    print(f'Contracted {datetime.datetime.now().timestamp()}')
    x = app.reqRealTimeBars(19001, contract, whatToShow='BID', useRTH=True, barSize=5, realTimeBarsOptions=[])
    print(f'Ordered {datetime.datetime.now().timestamp()}')
    time.sleep(5)
    app.disconnect()
    print(f'Disconnected {datetime.datetime.now().timestamp()}')
    time.sleep(2)

    return x
