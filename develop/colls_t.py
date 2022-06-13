import os
import sys
from datetime import datetime

import dateutil.relativedelta as relativedelta
import gspread as gd
import numpy as np
import pandas as pd
import pandas_ta as pta
import requests
import yfinance as yf


gc = gd.service_account(filename='../options-349716-50a9f6e13067.json')
worksheet = gc.open("work_table").worksheet('Активы с Call опционами')


d_len = worksheet.get('D2:D60')
q_len = worksheet.get('Q2:Q60')
a_tick = worksheet.get('A2:A60')

d = []
q = []
a = []
test = []
for i in range(len(d_len)):
    if (len(d_len[i])) == 1:
        d.append(d_len[i])
        q.append(q_len[i])
        q[i] = int(str(q[i])[2:-2])
        print(f"{d[i]}   {q[i]}")
    else:
        break

for i in range(len(a_tick)):
    a.append(a_tick[i])
    a[i] = str(a[i])[2:-2]
    if a[i] == 'Ticker':
        tmp = i
        break

print(a)

guru_tickers = []
for i in range(len(d)):
    if q[i] == 0:
        guru_tickers.append(str(d[i])[2:-2])

print(d)
print(len(d))
print(guru_tickers)
print(len(guru_tickers))



print(guru_tickers)


#guru_tickers = ['XPAR:UBI', 'HKSE:00941', 'HKSE:09988', 'NYSE:EBS', 'NAS:AEIS', 'NAS:VRTX', 'FRA:BAYN', 'NAS:CHKP', 'ARCA:GDX', 'ARCA:SLV', 'NYSE:TME', 'HKSE:01099', 'NAS:INTC', 'NYSE:EPD', 'ARCA:AMLP', 'NYSE:RIO', 'NYSE:BHP', 'NAS:STNE', 'NYSE:CI', 'NYSE:EVN', 'HKSE:00700', 'NAS:MKTX', 'NAS:GBOX', 'NAS:TER', 'NYSE:TDOC', 'MIL:DIA']

tickers_list = []
gf_list = []
TOTAL_DF = pd.DataFrame()

for tick in guru_tickers:
    print(tick)
    try:
        data_json = requests.get(
            f'https://api.gurufocus.com/public/user/34b27ff5b013ecb09d2eeafdf8724472:683d6c833f9571582151f19efe2278a8/stock/{tick}/summary').json()
        print(data_json)
        gf = data_json['summary']['company_data']['p2gf_value']

        tickers_list.append(tick)
        gf_list.append(float(gf))

    except Exception as e:
        tickers_list.append(tick)
        gf_list.append(0)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        pass

TOTAL_DF['Ticker'] = tickers_list
TOTAL_DF['Gf_value'] = gf_list

print('First')
print(TOTAL_DF)

listus = TOTAL_DF['Ticker'].tolist()
yahoo_list = []
j = 0
for num in range(len(listus)):
    print(j)
    print(listus[num])
    if "TSX:" in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.TO')
    elif 'TSXV:' in listus[num]:
         yahoo_list.append(listus[num].split(':')[1]+'.V')
    elif 'XSWX:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.SW')
    elif 'IST:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.IS')
    elif 'XKLS:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.KL')
    elif 'LSE:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.L')
    elif 'SGX:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.SI')
    elif 'HKSE:0' in listus[num]:
        yahoo_list.append(listus[num].replace('HKSE:0', '')+'.HK')
    elif 'ASX:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1]+'.AX')
    elif 'JSE:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.JO')
    elif 'XTAE:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.TA')
    elif 'TSE:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.T')
    elif 'MIC:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.ME')
        # elif 'MEX:' in listus[num]:
        #     yahoo_list.append(listus[num].split(':')[1]+'.MX')
    elif 'XMAD:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.MC')
    elif 'FRA:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.F')
    elif 'XAMS:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.AS')
    elif 'XBRU:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.BR')
    elif 'XPAR:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.PA')
    elif 'MIL:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1]+'.MI')
    elif 'NAS:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1])
    elif 'NYSE:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1])
    elif 'AMEX:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1])
    elif 'ARCA:' in listus[num]:
        yahoo_list.append(listus[num].split(':')[1])
    else:
        yahoo_list.append(listus[num])
    print(yahoo_list[-1])
    j += 1

std_list = []
std_30_list = []
rsi = []
i = 0
print(listus)
print(len(listus))
print(yahoo_list)
print(len(yahoo_list))

for tick in yahoo_list:
    try:
        today = datetime.today()
        start_vol = (datetime.today() - relativedelta.relativedelta(years=1))
        end_vol = today

        ohlc = pd.DataFrame()
        ohlc[tick] = yf.download(tick, start_vol, end_vol)['Close']

        log_df = pd.DataFrame()

        log_df['returns'] = np.log(ohlc[tick] / ohlc[tick].shift(-1))
        daily_std = np.std(log_df['returns'])
        std = (daily_std * 252 ** 0.5)

        days_30 = ohlc[tick][-10:]
        returns_30 = np.log(days_30 / days_30.shift(-1))
        daily_std_30 = np.std(returns_30)
        std_30 = (daily_std_30 * 252 ** 0.5)

        std_list.append(std)
        std_30_list.append(std_30)

        rsi_pr = pta.rsi(ohlc[tick])
        print(tick)
        print(rsi_pr[-1])
        i += 1
        rsi.append(rsi_pr[-1])

        tickers_list.append(tick)
    except:
        print(f'Ticker {tick} dont works today!')

print(rsi)
print(f"Total cycles {i}")
TOTAL_DF['RSI'] = rsi
TOTAL_DF['std'] = std_list
TOTAL_DF['std_30'] = std_30_list
TOTAL_DF['dif_vol'] = TOTAL_DF['std_30'] - TOTAL_DF['std']

TOTAL_DF = TOTAL_DF.sort_values('Gf_value', ascending=False).reset_index(drop=True)
TOTAL_DF['Gf_value_Score'] = TOTAL_DF.index.tolist()

TOTAL_DF = TOTAL_DF.sort_values('RSI',ascending=False).reset_index(drop=True)
TOTAL_DF['RSI_Score'] = TOTAL_DF.index.tolist()


TOTAL_DF = TOTAL_DF.sort_values('dif_vol',ascending=False).reset_index(drop=True)
TOTAL_DF['Vol_Score'] = TOTAL_DF.index.tolist()

TOTAL_DF['Total_score'] = TOTAL_DF['Gf_value_Score'] + TOTAL_DF['RSI_Score'] + TOTAL_DF['Vol_Score']
TOTAL_DF = TOTAL_DF.sort_values('Total_score').reset_index(drop=True)

print('Last')
print(TOTAL_DF)

print(TOTAL_DF['Ticker'][:10].tolist())


worksheet.update(f'A{tmp+3}', TOTAL_DF.values.tolist())

at = worksheet.get(f'A{tmp+3}:A{tmp+12}')

res = []
for i in range(len(at)):
    res.append(str(at[i]))
    res[i] = res[i][2:-2]

print(res)

