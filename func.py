import yfinance as yf
from datetime import date, timedelta
import gspread as gd
import datetime
import requests
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import pandas_ta as pta



def stock_price(ticker):

    #ticker = 'INTC'
    today = date.today()
    yesterday = today - timedelta(1)
    ohlc = yf.download(ticker, start=yesterday, end=today)['Adj Close']
    a = round(ohlc.iloc[0])

    return a


def options():

    gc = gd.service_account(filename='Seetzzz-1cb93f64d8d7.json')
    worksheet = gc.open("work_table").worksheet("Опционный портфель (short)")

    a_len = worksheet.get('A2:A60')
    a = []
    for i in range(len(a_len)):

        if (len(a_len[i])) == 1:
            a.append(a_len[i])
            #print(a_len[i])
        else:
            break

    print('Тикеры получены, определен диапазон работы.')
    add = str(len(a) + 1)
    s_dia = 'S2:S' + add
    t_dia = 'T2:T' + add
    s = worksheet.get(s_dia)
    t = worksheet.get(t_dia)
    print('Данные получены')
    a_arr = []
    s_arr = []
    t_arr = []

    for i in range(len(a)):
        a_arr.append(a[i])
        s_arr.append(s[i])
        if i >= len(t):
            t_arr.append(s[i])
        else:
            t_arr.append(t[i])
        if len(t_arr[i]) == 0:
            t_arr[i] = s_arr[i]

    for i in range(len(a)):
        try:
            a_arr[i] = str(a_arr[i])[2:-2]
            s_arr[i] = str(s_arr[i]).replace(',', '.')
            t_arr[i] = str(t_arr[i]).replace(',', '.')
            s_arr[i] = float(s_arr[i][2:-3])
            t_arr[i] = float(t_arr[i][2:-3])
        except:
            pass
    print('Данные преобразованы')

    result = []
    index = []
    for i in range(len(a_arr)):
        try:
            if s_arr[i] < t_arr[i]:
                result.append(a_arr[i])
                index.append(str(i+2))
        except:
            pass

    for i in range(len(result)):
        result[i] = str(result[i] + ' ' + index[i])

    return result


def rows_load():

    gc = gd.service_account(filename='Seetzzz-1cb93f64d8d7.json')
    worksheet = gc.open("work_table").worksheet("Опционный портфель (short)")

    a_len = worksheet.get('A2:A60')
    a = []
    for i in range(len(a_len)):

        if (len(a_len[i])) == 1:
            a.append(a_len[i])
        else:
            break

    add = len(a) + 1

    return add


def colls():

    gc = gd.service_account(filename='Seetzzz-1cb93f64d8d7.json')
    worksheet = gc.open("work_table").worksheet('Активы с Call опционами')

    a_tick = worksheet.get('A2:A60')
    a = []

    for i in range(len(a_tick)):
        a.append(a_tick[i])
        a[i] = str(a[i])[2:-2]
        if a[i] == 'Ticker':
            tmp = i
            break

    at = worksheet.get(f'A{tmp + 3}:A{tmp + 12}')
    res = []

    print("Данные получены")
    for i in range(len(at)):
        res.append(str(at[i]))
        res[i] = res[i][2:-2]

    return res


def fred_vix():
    today_d = datetime.date.today().strftime("%Y-%m-%d")
    six_mo = datetime.date.today() + relativedelta(months=-6)
    six_mo = six_mo.strftime("%Y-%m-%d")
    key = '97539e5b6d2151e8ff034e24e8410c89'
    url = 'https://api.stlouisfed.org/fred/series/observations'

    params1 = {
        'series_id': 'VIXCLS',
        'realtime_start': six_mo,
        'realtime_end': today_d,
        'observation_start': six_mo,
        'observation_end': today_d,
        'api_key': key,
        'file_type': 'json',
    }
    params2 = {
        'series_id': 'GVZCLS',
        'realtime_start': six_mo,
        'realtime_end': today_d,
        'observation_start': six_mo,
        'observation_end': today_d,
        'api_key': key,
        'file_type': 'json',
    }
    params3 = {
        'series_id': 'EVZCLS',
        'realtime_start': six_mo,
        'realtime_end': today_d,
        'observation_start': six_mo,
        'observation_end': today_d,
        'api_key': key,
        'file_type': 'json',
    }
    params4 = {
        'series_id': 'RVXCLS',
        'realtime_start': six_mo,
        'realtime_end': today_d,
        'observation_start': six_mo,
        'observation_end': today_d,
        'api_key': key,
        'file_type': 'json',
    }
    params5 = {
        'series_id': 'VXEEMCLS',
        'realtime_start': six_mo,
        'realtime_end': today_d,
        'observation_start': six_mo,
        'observation_end': today_d,
        'api_key': key,
        'file_type': 'json',
    }
    params6 = {
        'series_id': 'VXNCLS',
        'realtime_start': six_mo,
        'realtime_end': today_d,
        'observation_start': six_mo,
        'observation_end': today_d,
        'api_key': key,
        'file_type': 'json',
    }

    print('Начинаю загрузку данных')
    response1 = requests.get(url, params=params1)
    print('Got VIX')
    response2 = requests.get(url, params=params2)
    print('Got Gold')
    response3 = requests.get(url, params=params3)
    print('Got Euro')
    response4 = requests.get(url, params=params4)
    print('Got Russell')
    response5 = requests.get(url, params=params5)
    print('Got Emerging Markets')
    response6 = requests.get(url, params=params6)
    print('Got NASDAQ')

    print('Начинаю парсинг')
    rest1 = response1.json()['observations']
    vix_list = []
    date_vix = []
    for i in range(len(rest1)):
        if len(rest1[i]['value']) == 1:
            continue
        else:
            vix_list.append(float(rest1[i]['value']))
            date_vix.append(rest1[i]['date'])

    rest2 = response2.json()['observations']
    gold_list = []
    for i in range(len(rest2)):
        if len(rest2[i]['value']) == 1:
            continue
        else:
            gold_list.append(float(rest2[i]['value']))

    rest3 = response3.json()['observations']
    euro_list = []
    for i in range(len(rest3)):
        if len(rest3[i]['value']) == 1:
            continue
        else:
            euro_list.append(float(rest3[i]['value']))

    rest4 = response4.json()['observations']
    rassel_list = []
    for i in range(len(rest4)):
        if len(rest4[i]['value']) == 1:
            continue
        else:
            rassel_list.append(float(rest4[i]['value']))

    rest5 = response5.json()['observations']
    razv_list = []
    for i in range(len(rest5)):
        if len(rest5[i]['value']) == 1:
            continue
        else:
            razv_list.append(float(rest5[i]['value']))

    rest6 = response6.json()['observations']
    nasdaq_list = []
    date_nasdaq = []
    for i in range(len(rest6)):
        if len(rest6[i]['value']) == 1:
            continue
        else:
            nasdaq_list.append(float(rest6[i]['value']))
            date_nasdaq.append(rest6[i]['date'])

    print('Формирую RSI и VIX')
    DFV = pd.DataFrame(vix_list, index=date_vix, columns=['VIX'])

    DFV['RSI'] = pta.rsi(DFV['VIX'])
    DFV.dropna(inplace=True)

    DFN = pd.DataFrame(nasdaq_list, index=date_nasdaq, columns=['VIX'])

    DFN['RSI'] = pta.rsi(DFN['VIX'])
    DFN.dropna(inplace=True)

    VIX = {
        'current': vix_list[-1],
        'min': min(vix_list),
        'max': max(vix_list)
    }
    GOLD = {
        'current': gold_list[-1],
        'min': min(gold_list),
        'max': max(gold_list)
    }
    EUROPE = {
        'current': euro_list[-1],
        'min': min(euro_list),
        'max': max(euro_list)
    }
    RASSEL = {
        'current': rassel_list[-1],
        'min': min(rassel_list),
        'max': max(rassel_list)
    }
    RAZVITIE = {
        'current': razv_list[-1],
        'min': min(razv_list),
        'max': max(razv_list)
    }

    NASDAQ = {
        'current': nasdaq_list[-1],
        'min': min(nasdaq_list),
        'max': max(nasdaq_list)
    }

    RSI_VIX = {
        'current': round(DFV['RSI'][-1], 2),
        'min': round(DFV['RSI'].min(), 2),
        'max': round(DFV['RSI'].max(), 2)
    }

    RSI_NASDAQ = {
        'current': round(DFN['RSI'][-1], 2),
        'min': round(DFN['RSI'].min(), 2),
        'max': round(DFN['RSI'].max(), 2)
    }

    return VIX, GOLD, EUROPE, RASSEL, RAZVITIE, NASDAQ, RSI_VIX, RSI_NASDAQ


def china_vix():

    ticker = 'KWEB'
    end = datetime.date.today().strftime("%Y-%m-%d")
    start = datetime.date.today() + relativedelta(years=-1)
    start = start.strftime("%Y-%m-%d")

    print('Загружаю данные')
    ohlc = pd.DataFrame()
    ohlc['Adj Close'] = yf.download(ticker, start, end)['Adj Close']

    print('Обрабатываю данные')
    ohlc['returns'] = np.log(ohlc / ohlc.shift(-1))
    window_len = int(len(ohlc) / 2)
    gh = ohlc['returns'].rolling(window_len).std() * (252 ** 0.5)


    days_10 = ohlc['Adj Close'][-10:]
    ret_10 = np.log(days_10 / days_10.shift(-1))
    daily_std_10 = np.std(ret_10)
    std_10 = daily_std_10 * 252 ** .5
    CURRENT = round(std_10 * 100, 2)
    MAX = round(gh.max() * 100, 2)
    MIN = round(gh.min() * 100, 2)
    print('Отдаю данные')

    VIX_CHINA = {
        'current': CURRENT,
        'min': MIN,
        'max': MAX
    }

    return VIX_CHINA
