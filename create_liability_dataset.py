import pandas as pd
import warnings
import os
from multiprocessing import Pool

warnings.filterwarnings('ignore')

FOLDER_NAME = '{year}q{quarter}_notes'
INSTANCE_COLUMN = 'instance'
ADSH_COLUMN = 'adsh'
TAG_COLUMN = 'tag'
FOLDER = './'
INSTANCE_LIST = ['AAPL', 'SBUX', 'MSFT', 'CSCO', 'QCOM']
SUB_FILE = 'sub.tsv'
NUM_FILE = 'num.tsv'


def filter_adsh_index(df, instance):
    indices = df[df[INSTANCE_COLUMN].str.contains(instance.lower())].index.tolist()
    index = min(indices)
    return index


def obtain_adsh(filename, instance):
    adsh = None

    try:
        sub_df = pd.read_csv(filepath_or_buffer=filename, sep='\t')
        res_idx = filter_adsh_index(sub_df, instance)

        if res_idx:
            adsh = sub_df.loc[res_idx][ADSH_COLUMN]

    except Exception as ex:
        print('Error: {}'.format(ex))

    return adsh


def obtain_adsh_dict(folder_name):
    adsh_list = None
    for _, _, files in os.walk(folder_name):
        if SUB_FILE not in files:
            print('warning: {sub_file} does not exists in folder: {folder_name}' \
                  .format(sub_file=SUB_FILE, folder_name=folder_name))

        filename = folder_name + '/' + SUB_FILE
        adsh_list = list(map(lambda instance: obtain_adsh(filename, instance), INSTANCE_LIST))
    adsh_dict = dict(zip(INSTANCE_LIST, adsh_list))
    return adsh_dict


def obtain_liability_value(df, adsh_key):
    instance_liability_data = df[(df[ADSH_COLUMN] == adsh_key) & (df[TAG_COLUMN] == 'Liabilities')]
    liability = instance_liability_data[[ADSH_COLUMN, 'ddate', 'value']].groupby(ADSH_COLUMN).agg('max')['value']
    if not liability.empty:
        return liability.iloc[0]
    else:
        return None


def obtain_liability_dict(adsh_dict, folder_name):
    liability_dict = dict()
    filename = folder_name + '/' + NUM_FILE
    num_df = pd.read_csv(filepath_or_buffer=filename, sep='\t', encoding='ISO-8859-1')
    for instance, adsh_key in adsh_dict.items():
        liability = obtain_liability_value(num_df, adsh_key)
        liability_dict[instance] = liability
    return liability_dict


def obtain_liabilities(folder_name):
    date = folder_name.replace('_notes', '')
    adsh_dict = obtain_adsh_dict(folder_name)
    liability_dict = obtain_liability_dict(adsh_dict, folder_name)
    liability_dict['date'] = date
    return liability_dict


def write_results_to_csv(pool_results):
    results_list = [res.get() for res in pool_results]
    df = pd.DataFrame(data=results_list)
    df.to_csv('liabilities.csv')


def create_liability_dataset():
    pool = Pool(4)
    folders = [FOLDER_NAME.format(year=year, quarter=quarter) for year in range(2009, 2020) for quarter in range(1, 5)]
    pool_results = [pool.apply_async(obtain_liabilities, args=(folder, )) for folder in folders]
    write_results_to_csv(pool_results)


create_liability_dataset()


