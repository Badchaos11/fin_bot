import asyncio
import os
import sys

import aioschedule
import dateutil.relativedelta

from func import *
from ibapi_options import *
from main import bot


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

    print('Загружаю тикеры в гуру')
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
        else:
            yahoo_list.append(listus[num])

    std_list = []
    std_30_list = []
    rsi = []

    for tick in yahoo_list:
        try:
            today = datetime.datetime.today()
            start_vol = (datetime.datetime.today() - dateutil.relativedelta.relativedelta(years=1))
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
    #worksheet.update(f'A{update_from+3}', TOTAL_DF.values.tolist())
    print(TOTAL_DF['Ticker'])
    print("Обновление завершено")


async def vix_sender():
    usersFile = open("users.txt", "r")
    users = set()
    print('Загружаю данные')
    vix, gold, euro, rassel, emerg, nasdaq, rsi_vix, rsi_nasdaq = fred_vix()
    china = china_vix()
    print('Начинаю рассылку VIX')
    for line in usersFile:
        users.add(line.strip())
    usersFile.close()
    for user in users:
        print('Отправляю сообщение ' + str(user))
        await bot.send_message(user, f"VIX: {vix}")
        await bot.send_message(user, f"Gold VIX: {gold}")
        await bot.send_message(user, f"Euro VIX: {euro}")
        await bot.send_message(user, f"Rassel2000 VIX: {rassel}")
        await bot.send_message(user, f"Emerging VIX: {emerg}")
        await bot.send_message(user, f"China VIX: {china}")
        await bot.send_message(user, f"NASDAQ VIX: {nasdaq}")
        await bot.send_message(user, f"VIX RSI: {rsi_vix}")
        await bot.send_message(user, f"NASDAQ VIX: {rsi_nasdaq}")
    print('Рассылка завершена')


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


async def hourly_options():
    print(f'Starting update options')
    print(f'Updating shorts {datetime.datetime.now()}')
    shorts_update()
    print(f'Updating ETF {datetime.datetime.now()}')
    hedges('ETF')
    print(f'Updating Companies {datetime.datetime.now()}')
    hedges('Компании')
    print(f'Updating VIX {datetime.datetime.now()}')
    hedges('VIX')
    print(f'Finished updating at {datetime.datetime.now()}')

# Расписание на каждый день недели


async def scheduler():
    # Понедельник
    aioschedule.every().monday.at("12:00").do(vix_sender)

    aioschedule.every().monday.at("12:33").do(updater_colls)
    aioschedule.every().monday.at("12:35").do(sender)

    aioschedule.every().monday.at("16:33").do(updater_colls)
    aioschedule.every().monday.at("16:35").do(sender)

    aioschedule.every().monday.at("22:33").do(updater_colls)
    aioschedule.every().monday.at("22:35").do(sender)
    # Вторник
    aioschedule.every().tuesday.at("12:00").do(vix_sender)

    aioschedule.every().tuesday.at("12:33").do(updater_colls)
    aioschedule.every().tuesday.at("12:35").do(sender)

    aioschedule.every().tuesday.at("16:33").do(updater_colls)
    aioschedule.every().tuesday.at("16:35").do(sender)

    aioschedule.every().tuesday.at("22:33").do(updater_colls)
    aioschedule.every().tuesday.at("22:35").do(sender)
    # Среда
    aioschedule.every().wednesday.at("12:00").do(vix_sender)

    aioschedule.every().wednesday.at("12:33").do(updater_colls)
    aioschedule.every().wednesday.at("12:35").do(sender)

    aioschedule.every().wednesday.at("16:33").do(updater_colls)
    aioschedule.every().wednesday.at("16:35").do(sender)

    aioschedule.every().wednesday.at("22:33").do(updater_colls)
    aioschedule.every().wednesday.at("22:35").do(sender)
    # Четверг
    aioschedule.every().thursday.at("12:00").do(vix_sender)
    aioschedule.every().thursday.at("12:33").do(updater_colls)
    aioschedule.every().thursday.at("12:35").do(sender)
    aioschedule.every().thursday.at("16:33").do(updater_colls)
    aioschedule.every().thursday.at("16:35").do(sender)
    aioschedule.every().thursday.at("22:33").do(updater_colls)
    aioschedule.every().thursday.at("22:35").do(sender)
    # Пятница
    aioschedule.every().friday.at("12:00").do(vix_sender)
    aioschedule.every().friday.at("20:25").do(updater_colls)
    aioschedule.every().friday.at("14:56").do(sender)
    aioschedule.every().friday.at("16:33").do(updater_colls)
    aioschedule.every().friday.at("16:35").do(sender)
    aioschedule.every().friday.at("22:33").do(updater_colls)
    aioschedule.every().friday.at("22:35").do(sender)
    # Hourly update
    aioschedule.every().hour.at(':00').do(hourly_options)
    while True:  # Чтобы довавить дополнительную рассылку скопировать строку выше и выбрать время
        await aioschedule.run_pending()
        await asyncio.sleep(1)
