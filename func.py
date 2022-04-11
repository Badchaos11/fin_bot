import yfinance as yf
from datetime import date, timedelta
import gspread as gd
from time import sleep


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
        a_arr[i] = str(a_arr[i])[2:-2]
        s_arr[i] = str(s_arr[i]).replace(',', '.')
        t_arr[i] = str(t_arr[i]).replace(',', '.')
        s_arr[i] = float(s_arr[i][2:-3])
        t_arr[i] = float(t_arr[i][2:-3])

    print('Данные преобразованы')

    result = []
    index = []
    for i in range(len(a_arr)):
        if s_arr[i] < t_arr[i]:
            result.append(a_arr[i])
            index.append(str(i+2))

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
