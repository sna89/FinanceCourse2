
# coding: utf-8

# In[4]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def to_quarter(row):
    quarter = str(row['year'])+'q'+str(row['qtr_num'])
    return quarter

def str_quarter(row):
    period = row['Quarter']
    year = period.year
    quarter = period.quarter
    qtr = str(year) + 'q' + str(quarter)
    return qtr

def calc_equity(row):
    close = row['Close']
    shares = row['shares_in_milions']
    shares = shares.replace(',', '')
    shares = int(shares)
    return close*shares

def create_instance_stock_df(instance):
    instance_stock_df = pd.read_csv(instance + '.csv')
    instance_stock_df['Quarter'] = pd.PeriodIndex(pd.to_datetime(instance_stock_df['Date']), freq='Q')
    instance_stock_df['Date'] = pd.PeriodIndex(pd.to_datetime(instance_stock_df['Date']), freq='d')
    instance_stock_df = instance_stock_df.sort_values(by='Date',ascending=True)
    instance_stock_df['Str_Qtr'] = instance_stock_df.apply(str_quarter, axis=1)
    return instance_stock_df[['Date', 'Close', 'Quarter', 'Str_Qtr']]

def create_instance_shares_df(instance):
    csv_file_name = 'shares_{instance}.csv'.format(instance=instance)
    instance_shares_df = pd.read_csv(csv_file_name, names=['qtr_num','year','shares_in_milions'] )
    instance_shares_df['Str_Qtr'] = instance_shares_df.apply(to_quarter, axis=1)
    return instance_shares_df[['Str_Qtr', 'shares_in_milions']].sort_values(by='Str_Qtr',ascending=False)

INSTANCE_LIST = ['AAPL', 'SBUX', 'MSFT', 'CSCO', 'QCOM']

for instance in INSTANCE_LIST:
    instance = instance.lower()
    stock_df = create_instance_stock_df(instance)
    shares_df = create_instance_shares_df(instance)
    instance_stock_shares_df = pd.merge(stock_df, shares_df, how='inner', on=['Str_Qtr'])
    instance_stock_shares_df['Equity_in_milions'] = instance_stock_shares_df.apply(calc_equity, axis=1)
    instance_stock_shares_df[['Date','Str_Qtr','Close','shares_in_milions', 'Equity_in_milions']]
    instance_stock_shares_df.to_csv(instance + '_equity.csv')
    
equity_df = pd.DataFrame()
for instance in INSTANCE_LIST:
    instance = instance.lower()
    instance_stock_shares_df = pd.read_csv(instance + '_equity.csv')
    equity_df[instance] = instance_stock_shares_df['Equity_in_milions']
equity_df['Quarter'] = instance_stock_shares_df['Str_Qtr']
equity_df.index = instance_stock_shares_df['Date']
equity_df.to_csv('equities.csv')


# In[24]:




