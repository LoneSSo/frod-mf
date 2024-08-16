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
                                usecols=['MSISDN', '������� ����', '������', '�������� ����'])    
        if "������" in temp:
            temp.dropna(inplace = True, 
                        subset='������� ����')
            temp = temp.astype({'������� ����': 'int64'})
            if temp["������� ����"].isin([543079309570]).any():
                account_6404 = temp
                account_6404.drop(["���", "������"], 
                                axis = 1, 
                                inplace=True)
            elif temp["������� ����"].isin([560022423200]).any():
                account_8048 = temp
                try:
                    account_8048.drop(["���", "������"], 
                                    axis = 1, 
                                    inplace=True)
                except:
                    account_8048.drop(["������"], 
                                    axis = 1, 
                                    inplace=True)
            else:
                print(f"unknown data in {item}")
        elif "������� ����" in temp:
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
        flex64 = pd.read_csv(f'{yesterday}/report/���������� flex64.csv', usecols='MSISDN')['MSISDN'].to_list()
        if len(flex64) == 0:
            print('flex64:\n �� ����� ��� ������\n\n')
    except:
        flex64 = []
        print('flex64:\n �� ����� ��� ������\n\n')
    try:
        flex512 = pd.read_csv(f'{yesterday}/report/���������� flex512.csv', usecols='MSISDN')['MSISDN'].to_list()
        if len(flex512) == 0:
            print('flex512:\n �� ����� ��� ������\n\n')
    except:
        flex512 = []
        print('flex512:\n �� ����� ��� ������\n\n')

    try:
        roaming = pd.read_csv(f'{yesterday}/report/���������� �� �������.csv', usecols='�����')['�����'].to_list()
        if len(flex512) == 0:
            print('�������:\n �� ����� ��� ������\n\n')
    except:
        roaming = []
        print('�������:\n �� ����� ��� ������\n\n')

    try:
        limit = pd.read_csv(f'{yesterday}/report/��������� �����.csv', usecols='MSISDN')['MSISDN'].to_list()
        if len(limit) == 0:
            print('�������:\n �� ����� ��� ������\n\n')
    except:
        limit = []
        print('�������:\n �� ����� ��� ������\n\n')

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
    temp_data['�����'] = temp_data['�����'].str.replace(item, '')

temp_data['�����'] = temp_data['�����'].astype('int64')


roaming_expenses = temp_data.loc[temp_data['������ � ��������'] != 0][['�����', '������ � ��������']]
roaming_expenses.to_excel(f'{today}/report/���������� �� �������.xlsx', index=False)
roaming_expenses.to_csv(f'{today}/report/���������� �� �������.csv', index=False)
print('roaming check complete')


temp_data = temp_data.assign(������ = temp_data['������ � �������� �������'] + 
                            temp_data['������ � �������� �������'] + 
                            temp_data['������ � ��������'] + 
                            temp_data['������ �� ������'])
temp_data.drop(['������ � �������� �������', 
           '������ � ��������', 
           '������ � �������� �������', 
           '������ �� ������'], 
           inplace=True, 
           axis='columns')


temp_data.rename(columns={'�����' : 'MSISDN', '������� ����' : '�� ��'}, inplace=True)
data = pd.merge(crm_data, temp_data, on=['MSISDN'], how='left')
data = data[['MSISDN', 
             '������� ����', 
             '������', 
             '�������� ����', 
             '�� ��', 
             '�����', 
             '����.����� �� ��', 
             '���.������ � �������� �����', 
             '������� ����������', 
             '������']]
data['�� ��'] = data['�� ��'].fillna(0)
data = data.astype({'�� ��':'int64'})
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

flex64 = data.loc[data['�������� ����'].isin(['������� �������', 
                                              '������� �������.', 
                                              '������� ������������', 
                                              '������� ������������.', 
                                              '�����',
                                              '�����.'])]
flex64.reset_index(drop=True, inplace=True)

flex64 = flex64.loc[((flex64['�������� ����'] != '�����') & 
                        ((flex64['�� ��'].isin([543079309570, 560022423200]) & (flex64['������'] >= category_special)) | 
                        (~flex64['�� ��'].isin([543079309570, 560022423200]) & (flex64['������'] >= category_base))))  |
                    ((flex64['�������� ����'] == '�����') &
                        ((flex64['�� ��'].isin([543079309570, 560022423200]) & (flex64['������'] >= category_special)) | 
                        (~flex64['�� ��'].isin([543079309570, 560022423200]) & (flex64['������'] >= category_boost)))) ]
flex64 = flex64.loc[~flex64['MSISDN'].isin[yest_flex64]]

flex64.to_excel(f'{today}/report/���������� flex64.xlsx', index=False)
flex64.to_csv(f'{today}/report/���������� flex64.csv', index=False)
print('flex64 complete')

flex512 = data.loc[data['�������� ����'].isin(['������� ����������', 
                                              '������� ����������.', 
                                              'POS-Kassa �������'])]
flex512.reset_index(drop=True, inplace=True)
flex512 = flex512.loc[((flex512['�� ��'].isin([543079309570, 560022423200]) & (flex512['������'] >= category_special)) | 
                   (~flex512['�� ��'].isin([543079309570, 560022423200]) & (flex512['������'] >= category_boost)))]
flex512 = flex512.loc[~flex512['MSISDN'].isin(yest_flex512)]

flex512.to_excel(f'{today}/report/���������� flex512.xlsx', index=False)
flex512.to_csv(f'{today}/report/���������� flex512.csv', index=False)
print('iot70 complete')

limit800 = data.loc[data['�����'] >= 780]
limit800.to_excel(f'{today}/report/��������� �����.xlsx', index=False)
limit800.to_csv(f'{today}/report/��������� �����.csv', index=False)
print('limits complete')

click.pause('������. ����� ����� ������, ����� ������� ��� ����')


