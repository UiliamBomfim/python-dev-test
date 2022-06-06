import pandas as pd
import psycopg2
import math
import time
import sqlalchemy
#from sqlalchemy import null

first_insert = True 
init = 0

engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:postgres@localhost/airflow')


def extract_data1():
    data = pd.read_csv('/home/uiliam/Documentos/python-dev-test/data/Adult.data', header=None, sep=",",
                       on_bad_lines='skip',
                       names=['age', 'workclass', 'fnlwgt', 'education', 'education_num', 'marital_status',
                              'occupation',
                              'relationship', 'race', 'sex', 'capital_gain', 'capital_loss', 'hours_per_week',
                              'native_country', 'class'])

    return data


def extract_data2():
    teste = pd.read_csv('/home/uiliam/Documentos/python-dev-test/data/Adult.test', header=0, sep=",",
                        on_bad_lines='skip',
                        names=['age', 'workclass', 'fnlwgt', 'education', 'education_num', 'marital_status',
                               'occupation',
                               'relationship', 'race', 'sex', 'capital_gain', 'capital_loss', 'hours_per_week',
                               'native_country', 'class'])

    return teste


def transform_data():
    df_data = extract_data1()
    df_test = extract_data2()

    df_data['age'] = df_data['age'].apply(lambda x: int(x) if isinstance(x, int) else -1)
    df_data['fnlwgt'] = df_data['fnlwgt'].apply(lambda x: x.split(' ')[1])
    df_data['fnlwgt'] = df_data['fnlwgt'].apply(lambda x: int(x) if x.isdigit() else '')
    df_data['capital_gain'] = df_data['capital_gain'].apply(lambda x: x.split(' ')[1])
    df_data['capital_gain'] = df_data['capital_gain'].apply(lambda x: int(x) if x.isdigit() else -1)

    df_test['age'] = df_test['age'].apply(lambda x: int(x) if isinstance(x, int) else -1)
    df_test['hours_per_week'] = df_test['hours_per_week'].apply(lambda x: int(x) if x.isdigit() else '')
    df_test['hours_per_week'] = df_test['hours_per_week'].apply(lambda x: x.replace('?', ''))

    df_test['hours_per_week'] = df_test['hours_per_week'].apply(lambda x: x.split('.')[0])

    df = pd.concat([df_data, df_test])
    df = df.reset_index(drop=True)

    df['workclass'] = df['workclass'].apply(lambda x: x.replace('?', ''))
    df['education'] = df['education'].apply(lambda x: x.replace('?', ''))
    df['marital_status'] = df['marital_status'].apply(lambda x: x.replace('?', ''))
    df['occupation'] = df['occupation'].apply(lambda x: x.replace('?', ''))
    df['relationship'] = df['relationship'].apply(lambda x: x.replace('?', ''))
    df['race'] = df['race'].apply(lambda x: x.replace('?', ''))
    df['sex'] = df['sex'].apply(lambda x: x.replace('?', ''))
    df['native_country'] = df['native_country'].apply(lambda x: x.replace('?', ''))
    df['class'] = df['class'].apply(lambda x: x.replace('?', ''))

    return df


def populate_table():
    global first_insert
    global init
    df = transform_data()

    qtd = 1630

    if init == 0:
        df = df.iloc[:qtd]
        first_insert = False
        init = qtd

        df.to_sql('census', engine, index=False, if_exists='append')

        return init
    else:
        df = df.iloc[init:init + qtd]
        name = 'teste_uiliam'
        init = init + qtd

        df.to_sql('census', engine, index=False, if_exists='append')
        return init



      


