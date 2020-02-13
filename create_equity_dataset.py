
# coding: utf-8

# In[5]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# In[ ]:


INSTANCE_LIST = ['AAPL', 'SBUX', 'MSFT', 'CSCO', 'QCOM']


# In[ ]:


liability_df = pd.read_csv('liabilities.csv')

def plot_liability_data(liability_df):
    for instance in INSTANCE_LIST:
        instance_df = liability_df[[instance, 'date']]
        figure = plt.figure()
        ax = figure.add_axes([0.1, 0.1, 0.8, 0.8])
        plt.plot(instance_df[instance].values)
        x_ticks = [i*4 for i in range(11)]
        ax.set_xticks(x_ticks)
        x_tick_labels = [tick.replace('q1','') for tick in instance_df.date.values[x_ticks]]
        ax.set_xticklabels(x_tick_labels)
        plt.title('liabilities ' + instance)
        plt.show()

plot_liability_data(liability_df)


# In[ ]:


# compared with https://www.macrotrends.net/stocks/charts/SBUX/starbucks/total-liabilities and looks ok


# In[43]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

INSTANCE_LIST = ['AAPL', 'SBUX', 'MSFT', 'CSCO', 'QCOM']

def to_quarter(row):
    qtr = row['Qtr_raw']
    year = qtr[3:]
    q = qtr[1]
    quarter = year+'q'+q
    return quarter

def period_to_quarter(row):
    period = row['qtr_period']
    year = period.year
    quarter = period.quarter
    qtr = str(year) + 'q' + str(quarter)
    return qtr

def calc_equity(row):
    close = row[' Close/Last']
    close = close[2:]
    close = float(close)
    shares = row['shares_in_milions']
    shares = shares.replace(',', '')
    shares = int(shares)
    return close*shares

def create_instance_stock_df(instance):
    instance_stock_df = pd.read_csv(instance + '.csv')
    instance_stock_df['qtr_period'] = pd.PeriodIndex(pd.to_datetime(instance_stock_df['Date']), freq='Q')
    instance_stock_df['Date_datetime'] = pd.to_datetime(instance_stock_df['Date'])
    instance_stock_df = instance_stock_df[['Date_datetime', ' Close/Last', 'qtr_period']].groupby('qtr_period').max()
    instance_stock_df = instance_stock_df.sort_values(by='Date_datetime',ascending=False)
    instance_stock_df = instance_stock_df.reset_index()
    instance_stock_df['Qtr'] = instance_stock_df.apply(period_to_quarter, axis=1)
    return instance_stock_df

def create_instance_shares_df(instance):
    csv_file_name = 'shares_{instance}.csv'.format(instance=instance)
    instance_shares_df = pd.read_csv(csv_file_name, names=['Qtr_raw','shares_in_milions'] )
    instance_shares_df['Qtr'] = instance_shares_df.apply(to_quarter, axis=1)
    return instance_shares_df[['Qtr', 'shares_in_milions']].sort_values(by='Qtr',ascending=False)

for instance in INSTANCE_LIST:
    instance = instance.lower()
    stock_df = create_instance_stock_df(instance)
    shares_df = create_instance_shares_df(instance)
    instance_stock_shares_df = pd.merge(stock_df, shares_df, how='inner', on=['Qtr'])
    instance_stock_shares_df['Equity_in_milions'] = instance_stock_shares_df.apply(calc_equity, axis=1)
    instance_stock_shares_df[['Date_datetime','Qtr',' Close/Last','shares_in_milions', 'Equity_in_milions']]
    instance_stock_shares_df.to_csv(instance + '_equity.csv')


# In[47]:


equity_df = pd.DataFrame()
for instance in INSTANCE_LIST:
    instance = instance.lower()
    instance_stock_shares_df = pd.read_csv(instance + '_equity.csv')
    equity_df[instance] = instance_stock_shares_df['Equity_in_milions']
equity_df.index = instance_stock_shares_df['Qtr']
equity_df.to_csv('equities.csv')

