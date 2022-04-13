import yfinance as yf
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np

hk = pd.read_excel('Hong Kong (Hang Seng).xlsx', sheet_name='Hong Kong')
today_d = datetime.date.today().strftime("%Y-%m-%d")
print(today_d)
six_mo = datetime.date.today() + relativedelta(years=-1)
six_mo = six_mo.strftime("%Y-%m-%d")

tickers_list = []
for i in range(len(hk)):
    tickers_list.append(hk['ticker Guru'][i])
    tickers_list[i] = tickers_list[i][6:] + '.HK'
    #tickers_list[i] = tickers_list[i] + '.HK'

print(tickers_list)

ohlc = yf.download(tickers_list, start=six_mo, end=today_d)['Adj Close']
ohl = (np.log(ohlc / ohlc.shift(-1)))

vix = []
len_o = int(len(ohl) / 2)

for i in range(len_o + 1):
    ohl_y2 = ohl[i: len_o + i]
    china_ret = ohl_y2.min(axis=1)
    daily_m = china_ret.std()
    vix.append(daily_m)


df = pd.DataFrame(vix)

days_30 = ohlc[-10:].min(axis=1)
returns_30 = np.log(days_30 / days_30.shift(-1))
daily_std_30 = np.std(returns_30)
std_30 = (daily_std_30 * 252 ** 0.5)
print(vix)

print(std_30)

d10 = vix[-10:]
d10 = min(d10)
std10 = d10 * 252 ** .5

print(std10)

VIX_C1 = {
    'current': round(std_30, 4) * 100,
    'min': round(min(vix) * 252 ** .5, 4) * 100,
    'max':  round(max(vix) * 252 ** .5, 4) * 100
}

print(f"Current VIX: {VIX_C1['current']}")
print(f"Min VIX: {VIX_C1['min']}")
print(f"Max VIX: {VIX_C1['max']}")
'''
#print(ohl)


#print(ohl['china ret'])

#daily_ohl = np.std(ohl)
#daily_m = ohl['china ret'].std()

#print('VIX std')
#print(daily_m)
#print('VIX result last')
#vix_res = (daily_m * 252 ** 0.5)
#print(vix_res)

days_30 = ohl['china ret'][-10:]
returns_30 = np.log(days_30 / days_30.shift(-1))
daily_std_30 = np.std(returns_30)
std_30 = (daily_std_30 * 252 ** 0.5)


VIX_C1 = {
    'current': std_30,
    'min': vix.min() * 252 ** .5,
    'max': vix.max() * 252 ** .5
}

print(vix)

'''
