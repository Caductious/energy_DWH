import os

import psycopg2
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

load_dotenv()

postgres_engine = create_engine(f"postgresql+psycopg2://{os.getenv('PSQL_USER')}:{os.getenv('PSQL_PASSWORD')}@{os.getenv('PSQL_HOST')}:5432/{os.getenv('PSQL_DATABASE')}")

def load_table_to_dataframe(table_name, schema='silver', engine=postgres_engine):
    query = f'SELECT * FROM {schema}.{table_name}'
    print('Загрузка таблицы silver.energy_data')
    df = pd.read_sql(query, engine)
    print(f"Загружена таблица {schema}.{table_name} - {len(df)} строк")
    return df

def transformation(complete_df):
    recourse_mapping = {'electricity': 1, 'gas': 2}
    complete_df['recourse_id'] = complete_df['recourse'].map(recourse_mapping)
    location_cols = ['city', 'street', 'zipcode_from', 'zipcode_to', 'net_manager', 'purchase_area', 'type_of_connection', 'num_connections']
    complete_df['location_id'] = complete_df.groupby(location_cols).ngroup() + 1
    df_location = complete_df[location_cols + ['location_id']].drop_duplicates().reset_index(drop=True)
    df_recourse = pd.DataFrame({'recourse_id': [1, 2], 'recourse': ['electricity', 'gas']})
    df_fact = complete_df[['annual_consume', 'year', 'perc_of_active_connections',
                        'delivery_perc', 'type_conn_perc', 'location_id', 'recourse_id']].copy()
    df_fact['fact_id'] = range(1, len(df_fact) + 1)

    df_fact = df_fact[['fact_id', 'annual_consume', 'year', 'perc_of_active_connections',
                    'delivery_perc', 'type_conn_perc', 'location_id', 'recourse_id']]
    return df_fact, df_location, df_recourse

def load_data_to_gold(df_fact, df_location, df_recourse, postgres_engine):   
    try:
        print("\n1. Загрузка dim_recourse...")
        df_recourse.to_sql(
            name='dim_recourse',
            con=postgres_engine,
            schema='gold',
            if_exists='append',
            index=False,
            chunksize=10000
        )
        print(f"Загружено {len(df_recourse)} записей")
        
        print("\n2. Загрузка dim_location...")
        df_location.to_sql(
            name='dim_location',
            con=postgres_engine,
            schema='gold',
            if_exists='append',
            index=False,
            chunksize=10000
        )
        print(f"Загружено {len(df_location)} записей")
        
        print("\n3. Загрузка fact_table...")
        df_fact.to_sql(
            name='fact_table',
            con=postgres_engine,
            schema='gold',
            if_exists='append',
            index=False,
            chunksize=50000
        )
        print(f"Загружено {len(df_fact)} записей")        
        print("ЗАГРУЗКА УСПЕШНО ЗАВЕРШЕНА!")
        
    except Exception as e:
        print(f"\nОШИБКА ПРИ ЗАГРУЗКЕ: {e}")

df = load_table_to_dataframe('energy_data')
df_fact, df_location, df_recourse = transformation(df)
load_data_to_gold(df_fact, df_location, df_recourse, postgres_engine)
