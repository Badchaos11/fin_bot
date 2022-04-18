import yfinance as yf
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np

pd.options.mode.chained_assignment = None

ticker = 'KWEB'
end = datetime.date.today().strftime("%Y-%m-%d")
start = datetime.date.today() + relativedelta(years=-1)
start = start.strftime("%Y-%m-%d")

ohlc = pd.DataFrame()
ohlc['Adj Close'] = yf.download(ticker, start, end)['Adj Close']
print(ohlc)

print(ohlc)
ohlc['returns'] = np.log(ohlc/ohlc.shift(-1))
print(ohlc['returns'])
window_len = int(len(ohlc) / 2)
print(window_len)

gh = ohlc['returns'].rolling(window_len).std()*(252**0.5)
print(gh)


days_10 = ohlc['Adj Close'][-10:]
ret_10 = np.log(days_10/days_10.shift(-1))
daily_std_10 = np.std(ret_10)
std_10 = daily_std_10 * 252 ** .5
std_10 = round(std_10 * 100, 2)
print(std_10)
print(round(gh.min() * 100, 2))
