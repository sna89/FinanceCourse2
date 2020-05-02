
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

INSTANCE_LIST = ['AAPL', 'SBUX', 'MSFT', 'CSCO', 'QCOM']

def str_date_to_quarter(row):
    quarter = str(int(row['Year']))+'q'+str(int(row['Quarter']))
    return quarter

def date_to_quarter(row):
    period = row['Quarter']
    year = period.year
    quarter = period.quarter
    qtr = str(year) + 'q' + str(quarter)
    return qtr

def calc_equity(row):
    close = row['Close']
    shares = row['Shares']
    shares = shares.replace(',', '')
    shares = int(shares)
    return close*shares

def create_stock_df(instance):
    stock_df = pd.read_csv(instance + '.csv')
    stock_df['Quarter'] = pd.PeriodIndex(pd.to_datetime(stock_df['Date']), freq='Q')
    stock_df['Date'] = pd.to_datetime(stock_df['Date'])
    stock_df = stock_df[['Date', 'Close', 'Quarter']].groupby('Quarter').max()
    stock_df = stock_df.reset_index()
    stock_df['Quarter'] = stock_df.apply(date_to_quarter, axis=1)
    return stock_df

def create_shares_df(instance):
    csv_file_name = 'shares_{instance}.csv'.format(instance=instance)
    shares_df = pd.read_csv(csv_file_name)
    shares_df.rename(columns={'shares':'Shares'}, inplace=True)
    shares_df['Quarter'] = shares_df.apply(str_date_to_quarter, axis=1)
    return shares_df

for instance in INSTANCE_LIST:
    stock_df = create_stock_df(instance)
    shares_df = create_shares_df(instance)
    stock_shares_df = pd.merge(stock_df, shares_df, how='inner', on=['Quarter'])
    stock_shares_df['Equity_in_milions'] = stock_shares_df.apply(calc_equity, axis=1)
    stock_shares_df[['Date','Quarter', 'Close', 'Shares', 'Equity_in_milions']]
    stock_shares_df.to_csv(instance + '_equity.csv')

equity_df = pd.DataFrame()
for instance in INSTANCE_LIST:
    stock_shares_df = pd.read_csv(instance + '_equity.csv')
    equity_df[instance] = stock_shares_df['Equity_in_milions']
equity_df.index = stock_shares_df['Quarter']
equity_df.to_csv('equities.csv')

