import logging

from aiogram import Bot
from aiogram.dispatcher import Dispatcher
from aiogram.dispatcher.filters import Text
from aiogram.utils import executor

import config
from keyboard import *
from schedule import *

logging.basicConfig(level=logging.INFO)
pd.options.mode.chained_assignment = None


# Общая часть, рассылка сообщений и обновление документа


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
    vix, gold, euro, rassel, emerg, nasdaq, rsi_vix, rsi_nasdaq = fred_vix()
    china = china_vix()
    await bot.send_message(message.from_user.id, f"VIX: {vix}")
    await bot.send_message(message.from_user.id, f"Gold VIX: {gold}")
    await bot.send_message(message.from_user.id, f"Euro VIX: {euro}")
    await bot.send_message(message.from_user.id, f"Russell 2000 VIX: {rassel}")
    await bot.send_message(message.from_user.id, f"Emering VIX: {emerg}")
    await bot.send_message(message.from_user.id, f"China VIX: {china}")
    await bot.send_message(message.from_user.id, f"NASDAQ VIX: {nasdaq}")
    await bot.send_message(message.from_user.id, f"RSI VIX: {rsi_vix}")
    await bot.send_message(message.from_user.id, f"NASDAQ RSI: {rsi_nasdaq}")


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
