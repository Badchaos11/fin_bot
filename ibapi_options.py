import time

import gspread as gd
import pandas as pd

import ibapi_functions
from ibapi_functions import ask_checker, bid_checker


def shorts_update():
    gc = gd.service_account('options-349716-50a9f6e13067.json')

    worksheet = gc.open('work_table').worksheet('Опционный портфель (short)')

    options = []
    w = []
    print(f'Loading data for companies')
    for i in range(2, 100):
        x = worksheet.row_values(i)
        if len(x) == 0:
            print('Finished load data')
            break
        w.append(i)

        date = x[21]
        strike = x[9].replace(',', '.')
        date = date.split('.')
        rd = date[2] + date[1] + date[0]
        ticker = x[1]
        options.append([ticker, x[2], x[3], x[7], strike, x[14], rd, float(x[15])])
        print(options[-1])
        if i % 30 == 0:
            print('Need to wait 1 minute')
            time.sleep(60)

    print(options)
    time.sleep(60)

    ask_closes = []
    for option in options:
        try:
            print(option)
            x = ask_checker(option)
            if len(ibapi_functions.asks) != 0:
                print(f'Asks from f {ibapi_functions.asks}')
                ask_closes.append(ibapi_functions.asks[-1])
                ibapi_functions.asks.clear()
            else:
                print(f'Asks from f {ibapi_functions.asks}')
                ask_closes.append(None)
            time.sleep(1)
        except:
            print('smth went wrong')
            ask_closes.append(None)
            time.sleep(1)
            pass

    df = pd.DataFrame(data=options, columns=['Company', 'Currency', 'Exchange', 'Right', 'Strike', 'Multiplier',
                                             'Expiration Date', 'Number of Contracts'])
    df['Idx'] = w
    df = df.set_index('Idx')
    print(ask_closes)
    try:
        df['Ask Close'] = ask_closes
        df['Asks Ret'] = df['Number of Contracts'] * df['Ask Close']
    except:
        pass

    print('Result DF')
    print(df)
    df.to_csv('Test_CSV_Amer_09.csv')
    for row_number in w:
        if row_number % 30 == 0:
            print('Need to wait 1 min')
            time.sleep(60)
        if df['Asks Ret'][row_number] > 0:
            print(f"Insert {df['Asks Ret'][row_number]} into {row_number}")
            worksheet.update(f"L{row_number}", df['Asks Ret'][row_number])
        else:
            print(f'Nothing to insert into {row_number}')
    print('Wait 1 min before continue')
    time.sleep(60)


def hedges(sheet: str):
    gc = gd.service_account('options-349716-50a9f6e13067.json')

    worksheet = gc.open('Хэджи').worksheet(sheet)

    options = []
    w = []
    for i in range(2, 100):
        x = worksheet.row_values(i)
        if len(x) == 0:
            print('Finished load data')
            break

        w.append(i)
        date = x[5]
        strike = x[8].replace(',', '.')
        date = date.split('.')
        rd = date[2] + date[1] + date[0]
        ticker = x[1]
        if ticker.find(':') != -1:
            ticker = ticker.split(':')[1]
        tc = x[2]
        options.append([ticker, x[3], x[4], strike, x[6], rd, float(x[7]), tc])
        print(options[-1])
        if i % 40 == 0:
            print('Need to wait 1 minute')
            time.sleep(60)

    print(options)
    time.sleep(60)

    ask_closes = []
    if sheet == 'VIX':
        right = 'C'
    else:
        right = 'P'
    for option in options:
        try:
            print(option)
            x = bid_checker(option, right)
            if len(ibapi_functions.asks) != 0:
                print(f'Asks from f {ibapi_functions.asks}')
                ask_closes.append(ibapi_functions.asks[-1])
                ibapi_functions.asks.clear()
            else:
                print(f'Asks from f {ibapi_functions.asks}')
                ask_closes.append(None)
            time.sleep(1)
        except:
            print('smth went wrong')
            ask_closes.append(None)
            time.sleep(1)
            pass

    df = pd.DataFrame(data=options, columns=['Company', 'Currency', 'Exchange', 'Strike', 'Multiplier',
                                             'Expiration Date', 'Number of Contracts', 'Trade Class'])
    df['Idx'] = w
    df = df.set_index('Idx')
    print(ask_closes)
    df['Ask Close'] = ask_closes
    df['Asks Ret'] = df['Ask Close'] * df['Number of Contracts']

    print('Result')
    print(df)

    for row_number in w:
        if row_number % 30 == 0:
            print('Need to wait 1 min')
            time.sleep(60)
        if df['Asks Ret'][row_number] > 0:
            print(f"Insert {df['Asks Ret'][row_number]} into M{row_number}")
            worksheet.update(f"M{row_number}", df['Asks Ret'][row_number])
        else:
            print(f'Nothing to insert into M{row_number}')
    print('Wait 1 min before continue')
    time.sleep(60)
