import requests
import datetime
from dateutil.relativedelta import relativedelta

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

    response1 = requests.get(url, params=params1)
    print('Got it')
    response2 = requests.get(url, params=params2)
    print('Got it')
    response3 = requests.get(url, params=params3)
    print('Got it')
    response4 = requests.get(url, params=params4)
    print('Got it')
    response5 = requests.get(url, params=params5)
    print('Got it')

    print(response1.json())
    print(len(response1.json()))
    rest1 = response1.json()['observations']
    vix_list = []
    for i in range(len(rest1)):
        if len(rest1[i]['value']) == 1:
           continue
        else:
            vix_list.append(float(rest1[i]['value']))

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

    print(vix_list)
    print(gold_list)
    print(euro_list)
    print(rassel_list)
    print(razv_list)

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

    print(f'VIX: {VIX}')
    print(f'GOLD VIX: {GOLD}')
    print(f'EUROPE VIX: {EUROPE}')
    print(f'RASSEL VIX: {RASSEL}')
    print(f'RAZV VIX: {RAZVITIE}')

    return VIX, GOLD, EUROPE, RASSEL, RAZVITIE


res1, res2, res3, res4, res5 = fred_vix()

print(f'VIx: {res1}')
