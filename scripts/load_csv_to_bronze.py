import os
import sys

import secret

import pandas as pd
import psycopg2 
from psycopg2 import sql
import glob

def create_connection():
    connection = psycopg2.connect(host = secret.psql_host,
        database = secret.psql_database,
        user = secret.psql_user,
        password =secret.psql_password,
        port = "5432"
        )
    return connection

def get_table_name(csv_path):
    filename = os.path.basename(csv_path)
    table_name = filename.replace('.csv', '')
    return f"bronze.{table_name}"

def process_csv_file(csv_path, connection, batch_size=10000):
    try:
        table_name = get_table_name(csv_path)
        print(f"Обработка файла: {csv_path}")
        print(f"Имя таблицы: {table_name}")
        
        df = pd.read_csv(csv_path, low_memory=False)
        
        print(f"Прочитано {len(df)} строк, {len(df.columns)} колонок")
        print(f"Колонки: {df.columns.tolist()}")
        
        with connection.cursor() as cursor:
            connection.commit()
            
            drop_query = sql.SQL("DROP TABLE IF EXISTS {}").format(
                sql.Identifier(*table_name.split('.'))
            )
            cursor.execute(drop_query)
            connection.commit()
            
        from sqlalchemy import create_engine
        
        engine = create_engine(f"postgresql+psycopg2://{secret.psql_user}:{secret.psql_password}@{secret.psql_host}:5432/{secret.psql_database}")
        
        df.to_sql(
            name=table_name.split('.')[1],  # только имя таблицы без схемы
            con=engine,
            schema='bronze',
            if_exists='replace',
            index=False,
            chunksize=batch_size,
            method='multi'
        )
            
        print(f"Успешно загружено в таблицу: {table_name}")
        
    except Exception as e:
        print(f"Ошибка при обработке файла {csv_path}: {str(e)}")
        raise

def process_directory(directory_path):
    """Обработка всех CSV файлов в директории"""
    pattern = os.path.join(directory_path, "*_electricity_*.csv")
    csv_files = glob.glob(pattern)
    
    if not csv_files:
        print(f"Не найдено CSV файлов по шаблону: {pattern}")
        return
    
    print(f"Найдено {len(csv_files)} файлов для обработки")
    
    connection = create_connection()
    
    try:
        for csv_file in csv_files:
            process_csv_file(csv_file, connection)
            
        print(f"Все файлы успешно обработаны!")
    finally:
        connection.close()

process_directory('../Electricity')
