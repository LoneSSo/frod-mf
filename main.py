# -*- coding: cp1251 -*-

import pandas as pd
import numpy as np
import datetime
import click
import os


from chardet.universaldetector import UniversalDetector

detector = UniversalDetector()
def get_encode(source_file):

    with open(source_file, 'rb') as fh:
        for line in fh:
            detector.feed(line)
            if detector.done:
                break
        detector.close()
    return detector.result['encoding']

date = datetime.datetime.now()
yesterday = date - datetime.timedelta(days = 1)
today = date.strftime('%d-%m-%Y')

def ReadSrc(source_list):
    for item in source_list:
        try:
            temp = pd.read_csv(f"source/{item}", 
                            sep=";", 
                            encoding=get_encode(f"source/{item}"))
        except:
            temp = pd.read_excel(f"source/{item}", 
                                usecols=['MSISDN', 'Лицевой счет', 'Статус', 'Тарифный план'])    
        if "Период" in temp:
            temp.dropna(inplace = True, 
                        subset='Лицевой счет')
            temp = temp.astype({'Лицевой счет': 'int64'})
            if temp["Лицевой счет"].isin([543079309570]).any():
                account_6404 = temp
                account_6404.drop(["ФИО", "Период"], 
                                axis = 1, 
                                inplace=True)
            elif temp["Лицевой счет"].isin([560022423200]).any():
                account_8048 = temp
                try:
                    account_8048.drop(["ФИО", "Период"], 
                                    axis = 1, 
                                    inplace=True)
                except:
                    account_8048.drop(["Период"], 
                                    axis = 1, 
                                    inplace=True)
            else:
                print(f"unknown data in {item}")
        elif "Лицевой счет" in temp:
            temp['MSISDN'] = temp['MSISDN'].fillna(0)
            temp = temp.astype({'MSISDN': 'int64'})
            crm_data = temp
        else:
            print(f"invalid source {item}")

    result = pd.concat([account_6404, account_8048])
    result.reset_index(drop=True, inplace=True)
    result.dropna(axis = 1, inplace=True)

    return [result, crm_data]


def ReadYesterdayReport():
    try:
        flex64 = pd.read_csv(f'{yesterday}/report/Подключить flex64.csv', usecols='MSISDN')['MSISDN'].to_list()
        if len(flex64) == 0:
            print('flex64:\n За вчера нет данных\n\n')
    except:
        flex64 = []
        print('flex64:\n За вчера нет данных\n\n')
    try:
        flex512 = pd.read_csv(f'{yesterday}/report/Подключить flex512.csv', usecols='MSISDN')['MSISDN'].to_list()
        if len(flex512) == 0:
            print('flex512:\n За вчера нет данных\n\n')
    except:
        flex512 = []
        print('flex512:\n За вчера нет данных\n\n')

    try:
        roaming = pd.read_csv(f'{yesterday}/report/Начисления за роуминг.csv', usecols='Номер')['Номер'].to_list()
        if len(flex512) == 0:
            print('Роуминг:\n За вчера нет данных\n\n')
    except:
        roaming = []
        print('Роуминг:\n За вчера нет данных\n\n')

    try:
        limit = pd.read_csv(f'{yesterday}/report/Увеличить лимит.csv', usecols='MSISDN')['MSISDN'].to_list()
        if len(limit) == 0:
            print('Роуминг:\n За вчера нет данных\n\n')
    except:
        limit = []
        print('Роуминг:\n За вчера нет данных\n\n')

    return (flex64, flex512, roaming, limit)

source_list = os.listdir("source")
os.makedirs(f'{today}/data', exist_ok=True)
os.makedirs(f'{today}/report', exist_ok=True)

source = ReadSrc(source_list)
temp_data = source[0]
crm_data = source[1]
print('read complete')


to_change = [column for column in temp_data]
for i in range(2, len(to_change)):
    temp_data[to_change[i]] = temp_data[to_change[i]].str.replace(',', '.')
    temp_data[to_change[i]] = pd.to_numeric(temp_data[to_change[i]])


parts = ['+7(', ')', '-']
for item in parts:    
    temp_data['Номер'] = temp_data['Номер'].str.replace(item, '')

temp_data['Номер'] = temp_data['Номер'].astype('int64')


roaming_expenses = temp_data.loc[temp_data['Трафик в роуминге'] != 0][['Номер', 'Трафик в роуминге']]
roaming_expenses.to_excel(f'{today}/report/Начисления за роуминг.xlsx', index=False)
roaming_expenses.to_csv(f'{today}/report/Начисления за роуминг.csv', index=False)
print('roaming check complete')


temp_data = temp_data.assign(Трафик = temp_data['Трафик в домашнем регионе'] + 
                            temp_data['Трафик в домашнем филиале'] + 
                            temp_data['Трафик в роуминге'] + 
                            temp_data['Трафик по России'])
temp_data.drop(['Трафик в домашнем регионе', 
           'Трафик в роуминге', 
           'Трафик в домашнем филиале', 
           'Трафик по России'], 
           inplace=True, 
           axis='columns')


temp_data.rename(columns={'Номер' : 'MSISDN', 'Лицевой счет' : 'МФ ЛС'}, inplace=True)
data = pd.merge(crm_data, temp_data, on=['MSISDN'], how='left')
data = data[['MSISDN', 
             'Лицевой счет', 
             'Статус', 
             'Тарифный план', 
             'МФ ЛС', 
             'Всего', 
             'Абон.плата по ТП', 
             'Доп.услуги и тарифные опции', 
             'Разовые начисления', 
             'Трафик']]
data['МФ ЛС'] = data['МФ ЛС'].fillna(0)
data = data.astype({'МФ ЛС':'int64'})
data.reset_index(drop=True, inplace=True)


data.to_excel(f'{today}/data/data.xlsx', index=False)
data.to_csv(f'{today}/data/data.csv', index=False)

yesterday_tuple = ReadYesterdayReport()
yest_flex64 = yesterday_tuple[0]
yest_flex512 = yesterday_tuple[1]
yest_roaming = yesterday_tuple[2]
yest_limits = yesterday_tuple[3]


if date.day >= 0 and date.day <= 9:
    category_base = 70
    category_boost = 150
    category_special = 100
elif date.day >= 10 and date.day <= 19:
    category_base = 90
    category_boost = 200
    category_special = 100
elif date.day >= 20 and date.day <= 31:
    category_base = 120
    category_boost = 250
    category_special = 130

flex64 = data.loc[data['Тарифный план'].isin(['Зеленый Базовый', 
                                              'Зеленый Базовый.', 
                                              'Зеленый Классический', 
                                              'Зеленый Классический.', 
                                              'Зенит',
                                              'Зенит.'])]
flex64.reset_index(drop=True, inplace=True)

flex64 = flex64.loc[((flex64['Тарифный план'] != 'Зенит') & 
                        ((flex64['МФ ЛС'].isin([543079309570, 560022423200]) & (flex64['Трафик'] >= category_special)) | 
                        (~flex64['МФ ЛС'].isin([543079309570, 560022423200]) & (flex64['Трафик'] >= category_base))))  |
                    ((flex64['Тарифный план'] == 'Зенит') &
                        ((flex64['МФ ЛС'].isin([543079309570, 560022423200]) & (flex64['Трафик'] >= category_special)) | 
                        (~flex64['МФ ЛС'].isin([543079309570, 560022423200]) & (flex64['Трафик'] >= category_boost)))) ]
flex64 = flex64.loc[~flex64['MSISDN'].isin[yest_flex64]]

flex64.to_excel(f'{today}/report/Подключить flex64.xlsx', index=False)
flex64.to_csv(f'{today}/report/Подключить flex64.csv', index=False)
print('flex64 complete')

flex512 = data.loc[data['Тарифный план'].isin(['Зеленый Ускоренный', 
                                              'Зеленый Ускоренный.', 
                                              'POS-Kassa Зеленый'])]
flex512.reset_index(drop=True, inplace=True)
flex512 = flex512.loc[((flex512['МФ ЛС'].isin([543079309570, 560022423200]) & (flex512['Трафик'] >= category_special)) | 
                   (~flex512['МФ ЛС'].isin([543079309570, 560022423200]) & (flex512['Трафик'] >= category_boost)))]
flex512 = flex512.loc[~flex512['MSISDN'].isin(yest_flex512)]

flex512.to_excel(f'{today}/report/Подключить flex512.xlsx', index=False)
flex512.to_csv(f'{today}/report/Подключить flex512.csv', index=False)
print('iot70 complete')

limit800 = data.loc[data['Всего'] >= 780]
limit800.to_excel(f'{today}/report/Увеличить лимит.xlsx', index=False)
limit800.to_csv(f'{today}/report/Увеличить лимит.csv', index=False)
print('limits complete')

click.pause('Готово. Нажми любую кнопку, чтобы закрыть это окно')


