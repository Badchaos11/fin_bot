from func import *
import numpy as np
import pandas as pd
from aiogram import Bot
from aiogram.dispatcher import Dispatcher
from aiogram.dispatcher.filters import Text
from aiogram.utils import executor
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
import aioschedule
import httplib2
import mibian
pd.options.mode.chained_assignment = None


rows = rows_load()

print('Начинаю обновление документа')
LIST = 'Опционный портфель (short)'
CREDENTIALS_FILE = 'Seetzzz-1cb93f64d8d7.json'
spreadsheet_id = '1bfNJIgSEo9V5Jww1-EoUh_onba2bGY2LpDVx4aYlPzc'
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE,
                                                                   ['https://www.googleapis.com/auth/spreadsheets',
                                                                    'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = discovery.build('sheets', 'v4', http=httpAuth)
Option_WL = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f'{LIST}!A1:W10000',
                                                    majorDimension='COLUMNS').execute()
df3 = pd.DataFrame()
df3['Tickers'] = Option_WL['values'][0][1:rows]
df3['Option Type'] = Option_WL['values'][7][1:rows]
df3['Stock Price'] = Option_WL['values'][4][1:rows]
df3['Stock Price'] = df3['Stock Price'].str.replace(',', '.')
df3['Stock Price'] = df3['Stock Price'].astype(float)
df3['Strikes'] = Option_WL['values'][9][1:rows]
df3['Days_to_exp'] = Option_WL['values'][22][1:rows]
df3['Days_to_exp'] = df3['Days_to_exp'].astype(int)
df3['Ticker_Yahoo'] = Option_WL['values'][1][1:rows]
df3['Currency'] = Option_WL['values'][2][1:rows]
df3['Exchange'] = Option_WL['values'][3][1:rows]
df3['Strikes'] = df3['Strikes'].str.replace(',', '.')
df3['Strikes'] = df3['Strikes'].astype(float)
df3['Exp Date'] = Option_WL['values'][21][1:rows]
df3['Exp Date'] = df3['Exp Date'].str.replace('.', '-')
df3['Exp Date'] = pd.to_datetime(df3['Exp Date'], format='%d-%m-%Y')
df3['Exp Date'] = df3['Exp Date'].dt.date
df3['Current Premium'] = ''

print(df3.head())

print(df3)
print(yf.download('FB'))

for i in range(len(df3)):
    try:
        ticker = df3['Ticker_Yahoo'][i]

        ohlc = pd.DataFrame()
        ohlc[f'{ticker}'] = yf.download(ticker)['Close']
        days_30 = ohlc[f'{ticker}'][-10:]
        returns_30 = np.log(days_30 / days_30.shift(-1))
        daily_std_30 = np.std(returns_30)
        std_30 = daily_std_30 * 252 ** 0.5

        greeks = mibian.BS([df3['Stock Price'][i], df3['Strikes'][i], 0, df3['Days_to_exp'][i]],
                           volatility=std_30 * 100)
        if df3['Option Type'][i] == 'PUT':
            df3['Current Premium'][i] = round(greeks.putPrice, 2)
        elif df3['Option Type'][i] == 'CALL':
            df3['Current Premium'][i] = round(greeks.callPrice, 2)
    except:
        pass

    #gc = gd.service_account(filename='Seetzzz-1cb93f64d8d7.json')
    #worksheet = gc.open("work_table").worksheet("Опционный портфель (short)")
    #for i in range(len(df3)):
    #    try:
    #        worksheet.update(f"L{i + 2}", df3['Current Premium'][i])
    #    except:
    #        pass



print(df3)

test = options()
print(test)


