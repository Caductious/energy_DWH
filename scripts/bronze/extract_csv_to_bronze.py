import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2 
from psycopg2 import sql

load_dotenv()

def create_connection():
    connection = psycopg2.connect(
        host=os.getenv('PSQL_HOST'),
        database=os.getenv('PSQL_DATABASE'),
        user=os.getenv('PSQL_USER'),
        password=os.getenv('PSQL_PASSWORD'),
        port="5432"
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
        
        engine = create_engine(f"postgresql+psycopg2://{os.getenv('PSQL_USER')}:{os.getenv('PSQL_PASSWORD')}@{os.getenv('PSQL_HOST')}:5432/{os.getenv('PSQL_DATABASE')}")
        
        df.to_sql(
            name=table_name.split('.')[1], 
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

def find_csv_files(directory_path):
    csv_files = []
    if not os.path.exists(directory_path):
        print(f"Директория не существует: {directory_path}")
        return csv_files  
    try:
        items = os.listdir(directory_path)
        for item in items:
            full_path = os.path.join(directory_path, item)
            
            if os.path.isfile(full_path):
                if item.lower().endswith('.csv'):
                    if '_electricity_' in item:
                        csv_files.append(full_path)
    except Exception as e:
        print(f"Ошибка при сканировании директории {directory_path}: {str(e)}")   
    return csv_files

def process_directory(directory_path):
    csv_files = find_csv_files(directory_path)
    
    if not csv_files:
        print(f"Не найдено CSV файлов по шаблону '*_electricity_*.csv' в директории: {directory_path}")
        return
    
    print(f"Найдено {len(csv_files)} файлов для обработки:")
    for file in csv_files:
        print(f"  - {os.path.basename(file)}")
    
    connection = create_connection()  
    try:
        for csv_file in csv_files:
            process_csv_file(csv_file, connection)         
        print(f"Все файлы успешно обработаны!")
    finally:
        connection.close()

process_directory('Electricity')
