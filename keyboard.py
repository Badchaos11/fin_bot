from aiogram import types

keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
buttons = ["Посмотреть опционы", "Цена акции"]
keyboard.add(*buttons)

stocks = types.ReplyKeyboardMarkup(resize_keyboard=True)
buttons_s = ["INTC", "AAPL", "CRL", "AMGN", "GILD"]
stocks.add(*buttons_s)
