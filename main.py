from aiogram import Bot
from aiogram.utils import executor
from aiogram.dispatcher import Dispatcher
from aiogram.dispatcher.filters import Text
from keyboard import *
import logging
import pandas as pd
import numpy as np
from func import *
import aioschedule
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import mibian
import asyncio
import config

logging.basicConfig(level=logging.INFO)


# Общая часть, рассылка сообщений


async def updater():
    rows = rows_load()  # Внимательно, заполнить количество строк!!!!!!!

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
    df3.head()

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

    gc = gd.service_account(filename='Seetzzz-1cb93f64d8d7.json')
    worksheet = gc.open("work_table").worksheet("Опционный портфель (short)")
    for i in range(len(df3)):
        try:
            worksheet.update(f"L{i + 2}", df3['Current Premium'][i])
        except:
            pass
    #print(df3)

    print('Обновление завершено')


async def sender():
    joinedFile = open("users.txt", "r")
    joinedUsers = set()
    result = options()
    print('Начинаю рассылку')
    for line in joinedFile:
        joinedUsers.add(line.strip())
    joinedFile.close()
    for user in joinedUsers:
        try:
            print('Отправляю сообщение ' + str(user))
            await bot.send_message(user, f"Доходность при продаже по текущей цене выше у следующих компаний"
                                         f" (компания и номер строки):\n"
                                         f" {result}")
            await asyncio.sleep(1)
        except:
            pass
    print('Рассылка зваершена')


async def scheduler():
    aioschedule.every().day.at("12:30").do(updater)  # Для выбора времени рассылки изменить чисто в скобках после at
    aioschedule.every().day.at("12:35").do(sender)
    aioschedule.every().day.at("16:30").do(updater)
    aioschedule.every().day.at("16:35").do(sender)  # Для выбора времени рассылки изменить чисто в скобках после at
    aioschedule.every().day.at("22:30").do(updater)
    aioschedule.every().day.at("22:35").do(sender)
    while True:  # Чтобы довавить дополнительную рассылку скопировать строку выше и выбрать время
        await aioschedule.run_pending()
        await asyncio.sleep(1)


async def on_startup(x):
    asyncio.create_task(scheduler())


bot = Bot(token=config.botkey)
dp = Dispatcher(bot)


# Часть клиента
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
async def welcome(message: types.Message):
    await bot.send_message(message.from_user.id, "Меня создали для того, чтобы смотреть документ гугл таблиц.\n"
                                                 "Но возможно я буду делать больше.")


@dp.message_handler(Text(equals="Посмотреть опционы"))
async def with_puree(message: types.Message):
    await bot.send_message(message.from_user.id, "Подождите немного, я проверяю документ и творю магию.")
    await bot.send_message(message.from_user.id, f"Доходность при продаже по текущей цене выше у следующих компаний"
                                                 f" (компания и номер строки):\n"
                                                 f" {options()}")
    await bot.send_message(message.from_user.id, "Я всё сделал. Что же дальше?")


@dp.message_handler(Text(equals="Цена акции"))
async def with_puree(message: types.Message):
    await bot.send_message(message.from_user.id,
                           "Цену для какой компании вы хотите? Пока их мало, но скоро будет больше.",
                           reply_markup=stocks)


@dp.message_handler(Text(equals=["INTC", "AAPL", "CRL", "AMGN", "GILD"]))
async def with_puree(message: types.Message):
    await bot.send_message(message.from_user.id, "Цена на акцию равняется " + str(stock_price(message.text)))


@dp.message_handler(Text(equals="В начало"))
async def to_start(message: types.Message):
    await bot.send_message(message.from_user.id, "Снова сначала? Чем займёмся?",
                           reply_markup=keyboard)


# Старт бота
if __name__ == '__main__':
    print('Танец бешеной белки начинается!')
    executor.start_polling(dp, skip_updates=False, on_startup=on_startup)
