import os

import numpy as np
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from progress.bar import IncrementalBar
from dotenv import load_dotenv

load_dotenv()

postgres_engine = create_engine(f"postgresql+psycopg2://{os.getenv('PSQL_USER')}:{os.getenv('PSQL_PASSWORD')}@{os.getenv('PSQL_HOST')}:5432/{os.getenv('PSQL_DATABASE')}")

def get_all_tables(engine):
    inspector = inspect(engine)
    return inspector.get_table_names(schema='bronze')

def load_tables_from_bronze(postgres_engine):
    electric_list = []
    gas_list =[]
    bar = IncrementalBar('Загрузка таблиц', max=len(all_tables))
    for table_name in all_tables:
        if 'electricity' in table_name:
            query = f'SELECT * FROM bronze."{table_name}"'
            df = pd.read_sql_query(query, postgres_engine)
            df['year'] = table_name[-4:]
            df['recourse'] = 'electricity'
            electric_list.append(df)
            bar.next()

        elif 'mapping' in table_name:
            query = f'SELECT * FROM bronze."{table_name}"'
            mapping_df = pd.read_sql_query(query, postgres_engine)
            bar.next()

        elif 'mysql' in table_name:
            if 'mapping' not in table_name:
                query = f'SELECT * FROM bronze."{table_name}"'
                df = pd.read_sql_query(query, postgres_engine)
                df['recourse'] = 'gas'
                gas_list.append(df)
                bar.next()
    bar.finish()
    electric_df = pd.concat(electric_list, ignore_index=True)
    electric_df = electric_df.drop(['smartmeter_perc', '%Defintieve aansl (NRM)', 'STANDAARDDEVIATIE'], axis=1)

    gas_df = pd.concat(gas_list, ignore_index=True)
    gas_df = pd.merge(
        gas_df,
        mapping_df,
        on='manager_id',
        how='left',
        suffixes=('_gas', '_mapping')
    )
    gas_df = gas_df.drop(['manager_id', 'id'], axis=1)
    gas_df = gas_df.rename(columns = {"manager_name":"net_manager"})
    complete_df = pd.concat([gas_df, electric_df], ignore_index=True)
    return complete_df

def transform(complete_df):
    complete_df = complete_df.drop_duplicates()
    complete_df[['delivery_perc', 'type_conn_perc', 'year']] = complete_df[['delivery_perc', 'type_conn_perc', 'year']].apply(pd.to_numeric, errors='coerce')
    print(f"Объединенный датафрейм создан. Размер: {len(complete_df)} строк, {len(complete_df.columns)} столбцов")
    print(complete_df.info())
    numeric_columns = complete_df.select_dtypes(include=[np.number]).columns
    complete_df[numeric_columns] = complete_df[numeric_columns].apply(lambda x: x.fillna(x.median()))
    complete_df[['type_of_connection', 'purchase_area', 'zipcode_from']] = complete_df[['type_of_connection', 'purchase_area', 'zipcode_from']].fillna('unknown')
    print("Пропущенные численные значения были заполнены медианой, текстовые на 'unknown'сумма пустых строк в каждом столбце:")
    print(complete_df.isna().sum())
    return complete_df
    
def save_to_silver(df, engine, chunksize = 500000):
    bar = IncrementalBar(f'Загрузка таблицы в PostgreSQL размер чанка={chunksize}', max=1+(len(df)/chunksize))
    df.head(chunksize).to_sql(
    name="energy_data",
    con=engine,
    schema='silver',
    if_exists='replace',
    index=False
    )
    bar.next()
    for start in range(chunksize, len(df), chunksize):
        end = min(start + chunksize, len(df))
        chunk = df.iloc[start:end]     
        chunk.to_sql(
            name="energy_data",
            con=engine,
            schema='silver',
            if_exists='append',
            index=False
         )
        bar.next()
    bar.finish()
all_tables = get_all_tables(postgres_engine)
df_silver = load_tables_from_bronze(postgres_engine)
df_silver = transform(df_silver)
save_to_silver(df_silver, postgres_engine)