import asyncio
import logging
import sys
import os

import aioschedule
from datetime import datetime
import httplib2
import mibian
import numpy as np
import pandas as pd
from aiogram import Bot
from aiogram.dispatcher import Dispatcher
from aiogram.dispatcher.filters import Text
from aiogram.utils import executor
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
import pandas_ta as pta
import dateutil.relativedelta as relativedelta
import requests

import config
from func import *
from keyboard import *

logging.basicConfig(level=logging.INFO)
pd.options.mode.chained_assignment = None


# Общая часть, рассылка сообщений и обновление документа


async def updater():
    rows = rows_load()  # Внимательно, заполнить количество строк!!!!!!!

    print('Начинаю обновление документа')
    print('Загрузка данных')
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
    df3.head()

    #print(df3)

    #print(yf.download('FB'))
    print('Получение данных Яху')
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

    print('Начинаю заполнение документа')
    gc = gd.service_account(filename='Seetzzz-1cb93f64d8d7.json')
    worksheet = gc.open("work_table").worksheet("Опционный портфель (short)")
    for i in range(len(df3)):
        try:
            worksheet.update(f"L{i + 2}", df3['Current Premium'][i])
        except:
            pass
    #print(df3)

    print('Обновление завершено')


async def updater_colls():

    print("Начинаю обновление целей")
    gc = gd.service_account(filename='Seetzzz-1cb93f64d8d7.json')
    worksheet = gc.open("work_table").worksheet('Активы с Call опционами')

    d_len = worksheet.get('D2:D60')
    q_len = worksheet.get('Q2:Q60')
    a_tick = worksheet.get('A2:A60')
    d = []
    q = []
    a = []

    for i in range(len(d_len)):
        if (len(d_len[i])) == 1:
            d.append(d_len[i])
            q.append(q_len[i])
            q[i] = int(str(q[i])[2:-2])
        else:
            break

    for i in range(len(a_tick)):
        a.append(a_tick[i])
        a[i] = str(a[i])[2:-2]
        if a[i] == 'Ticker':
            update_from = i
            break

    guru_tickers = []
    for i in range(len(d)):
        if q[i] == 0:
            guru_tickers.append(str(d[i])[2:-2])

    tickers_list = []
    gf_list = []
    TOTAL_DF = pd.DataFrame()

    for tick in guru_tickers:
        print(tick)
        try:
            data_json = requests.get(
                f'https://api.gurufocus.com/public/user/34b27ff5b013ecb09d2eeafdf8724472:683d6c833f9571582151f19efe2278a8/stock/{tick}/summary').json()

            gf = data_json['summary']['company_data']['p2gf_value']

            tickers_list.append(tick)
            gf_list.append(float(gf))

        except Exception as e:
            tickers_list.append(tick)
            gf_list.append(0)
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            pass

    TOTAL_DF['Ticker'] = tickers_list
    TOTAL_DF['Gf_value'] = gf_list

    print("Загружено GF")
    listus = TOTAL_DF['Ticker'].tolist()
    yahoo_list = []
    for num in range(len(listus)):
        if "TSX:" in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.TO')
        elif 'TSXV:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.V')
        elif 'XSWX:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.SW')
        elif 'IST:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.IS')
        elif 'XKLS:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.KL')
        elif 'LSE:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.L')
        elif 'SGX:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.SI')
        elif 'HKSE:0' in listus[num]:
            yahoo_list.append(listus[num].replace('HKSE:0', '') + '.HK')
        elif 'ASX:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.AX')
        elif 'JSE:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.JO')
        elif 'XTAE:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.TA')
        elif 'TSE:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.T')
        elif 'MIC:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.ME')
        # elif 'MEX:' in listus[num]:
        #     yahoo_list.append(listus[num].split(':')[1]+'.MX')
        elif 'XMAD:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.MC')
        elif 'FRA:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.F')
        elif 'XAMS:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.AS')
        elif 'XBRU:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.BR')
        elif 'XPAR:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.PA')
        elif 'MIL:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1] + '.MI')
        elif 'NAS:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1])
        elif 'NYSE:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1])
        elif 'AMEX:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1])
        elif 'ARCA:' in listus[num]:
            yahoo_list.append(listus[num].split(':')[1])

    std_list = []
    std_30_list = []
    rsi = []

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
            rsi.append(rsi_pr[-1])

            tickers_list.append(tick)
        except:
            pass

    print("Загружено остальное")
    TOTAL_DF['RSI'] = rsi
    TOTAL_DF['std'] = std_list
    TOTAL_DF['std_30'] = std_30_list
    TOTAL_DF['dif_vol'] = TOTAL_DF['std_30'] - TOTAL_DF['std']

    TOTAL_DF = TOTAL_DF.sort_values('Gf_value', ascending=False).reset_index(drop=True)
    TOTAL_DF['Gf_value_Score'] = TOTAL_DF.index.tolist()

    TOTAL_DF = TOTAL_DF.sort_values('RSI', ascending=False).reset_index(drop=True)
    TOTAL_DF['RSI_Score'] = TOTAL_DF.index.tolist()

    TOTAL_DF = TOTAL_DF.sort_values('dif_vol', ascending=False).reset_index(drop=True)
    TOTAL_DF['Vol_Score'] = TOTAL_DF.index.tolist()

    TOTAL_DF['Total_score'] = TOTAL_DF['Gf_value_Score'] + TOTAL_DF['RSI_Score'] + TOTAL_DF['Vol_Score']
    TOTAL_DF = TOTAL_DF.sort_values('Total_score').reset_index(drop=True)

    print("Обновляю документ")
    worksheet.update(f'A{update_from+3}', TOTAL_DF.values.tolist())
    print("Обновление завершено")


async def vix_sender():
    joinedFile = open("users.txt", "r")
    joinedUsers = set()
    vix, gold, euro, rassel, emerg = fred_vix()
    china = china_vix()
    print('Начинаю рассылку VIX')
    for line in joinedFile:
        joinedUsers.add(line.strip())
    joinedFile.close()
    for user in joinedUsers:
        print('Отправляю сообщение ' + str(user))
        await bot.send_message(user, f"VIX: {vix}")
        await bot.send_message(user, f"Gold VIX: {gold}")
        await bot.send_message(user, f"Euro VIX: {euro}")
        await bot.send_message(user, f"Rassel2000 VIX: {rassel}")
        await bot.send_message(user, f"Emerging VIX: {emerg}")
        await bot.send_message(user, f"China VIX: {china}")


async def sender():
    joinedFile = open("users.txt", "r")
    joinedUsers = set()
    result_o = options()
    result_c = colls()
    print('Начинаю рассылку')
    for line in joinedFile:
        joinedUsers.add(line.strip())
    joinedFile.close()
    for user in joinedUsers:
        try:
            print('Отправляю сообщение ' + str(user))
            await bot.send_message(user, f"Доходность при продаже по текущей цене выше у следующих компаний"
                                         f" (компания и номер строки):\n"
                                         f" {result_o}")
            await bot.send_message(user, f"Лучшие цели для колов:\n"
                                         f" {result_c}")
            await asyncio.sleep(1)
        except:
            pass
    print('Рассылка зваершена')

# Изменить время отправки на серверное
async def scheduler():
    aioschedule.every().day.at("12:00").do(vix_sender)
    aioschedule.every().day.at("12:30").do(updater)  # Для выбора времени рассылки изменить чисто в скобках после at
    aioschedule.every().day.at("12:33").do(updater_colls)
    aioschedule.every().day.at("12:35").do(sender)
    aioschedule.every().day.at("16:30").do(updater)
    aioschedule.every().day.at("16:33").do(updater_colls)
    aioschedule.every().day.at("17:15").do(sender)  # Для выбора времени рассылки изменить чисто в скобках после at
    aioschedule.every().day.at("22:30").do(updater)
    aioschedule.every().day.at("22:33").do(updater_colls)
    aioschedule.every().day.at("22:35").do(sender)
    while True:  # Чтобы довавить дополнительную рассылку скопировать строку выше и выбрать время
        await aioschedule.run_pending()
        await asyncio.sleep(1)


async def on_startup(x):
    asyncio.create_task(scheduler())


bot = Bot(token=config.botkey)
dp = Dispatcher(bot)


# Часть клиента, команды и сообщения
@dp.message_handler(commands="start")
async def welcome(message: types.Message):
    joinedFile = open("users.txt", "r")
    joinedUsers = set()
    for line in joinedFile:
        joinedUsers.add(line.strip())

    if not str(message.chat.id) in joinedUsers:
        joinedFile = open("users.txt", "a")
        joinedFile.write(str(message.chat.id) + "\n")
        joinedUsers.add(message.chat.id)

    await message.answer("Приветствую, " + message.from_user.first_name)
    await bot.send_message(message.from_user.id, "Что будем делать дальше? Посмотрим гугл док?",
                           reply_markup=keyboard)


@dp.message_handler(commands="Что")
async def what(message: types.Message):
    await bot.send_message(message.from_user.id, "Меня создали для того, чтобы смотреть документ гугл таблиц.\n"
                                                 "Но возможно я буду делать больше.")


@dp.message_handler(Text(equals="Посмотреть опционы"))
async def see_options(message: types.Message):
    await bot.send_message(message.from_user.id, "Подождите немного, я проверяю документ и творю магию.")
    await bot.send_message(message.from_user.id, f"Доходность при продаже по текущей цене выше у следующих компаний"
                                                 f" (компания и номер строки):\n"
                                                 f" {options()}")
    await bot.send_message(message.from_user.id, "Я всё сделал. Что же дальше?")


@dp.message_handler(Text(equals="Цели для колов"))
async def see_colls(message: types.Message):
    await bot.send_message(message.from_user.id, "Подождите неиного, я здесь колдую.")
    await bot.send_message(message.from_user.id, f"Лучшие цели для колов:\n"
                                                 f" {colls()}")


@dp.message_handler(Text(equals="Цена акции"))
async def stock_pr(message: types.Message):
    await bot.send_message(message.from_user.id,
                           "Цену для какой компании вы хотите? Пока их мало, но скоро будет больше.",
                           reply_markup=stocks)


@dp.message_handler(Text(equals='VIX'))
async def vix_values(message: types.Message):
    await bot.send_message(message.from_user.id, "Подождите немного, загружаю данные")
    vix, gold, euro, rassel, emerg = fred_vix()
    china = china_vix()
    await bot.send_message(message.from_user.id, f"VIX: {vix}")
    await bot.send_message(message.from_user.id, f"Gold VIX: {gold}")
    await bot.send_message(message.from_user.id, f"Euro VIX: {euro}")
    await bot.send_message(message.from_user.id, f"Rassel2000 VIX: {rassel}")
    await bot.send_message(message.from_user.id, f"Emering VIX: {emerg}")
    await bot.send_message(message.from_user.id, f"China VIX: {china}")


@dp.message_handler(Text(equals=["INTC", "AAPL", "CRL", "AMGN", "GILD"]))
async def stock_ch(message: types.Message):
    await bot.send_message(message.from_user.id, "Цена на акцию равняется " + str(stock_price(message.text)))


@dp.message_handler(Text(equals="В начало"))
async def to_start(message: types.Message):
    await bot.send_message(message.from_user.id, "Снова сначала? Чем займёмся?",
                           reply_markup=keyboard)


# Старт бота
if __name__ == '__main__':
    print('Танец бешеной белки начинается!')
    executor.start_polling(dp, skip_updates=False, on_startup=on_startup)
