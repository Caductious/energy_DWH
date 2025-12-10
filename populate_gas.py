import os

import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
from progress.bar import IncrementalBar

import secret

#переименовываем неправильно названные файлы
def rename_files(path):
    for filename in os.listdir(path):
        namelist = filename.split('_')
        if namelist[1] != 'gas':
            namelist[0] = namelist[0][:-3]
            namelist.append(namelist[1])
            namelist[1] = 'gas'
            new_name = '_'.join(namelist)
            os.rename(path + filename, path + {new_name})

#Загрузка и обработка датафрейма
def load_dataframes(path):
    df_list=[]
    for filename in os.listdir('./Gas'):
        df = pd.read_csv(f"./Gas/{filename}")
        df['year'] = int(filename[-8:-4])
        df_list.append(df)
        if 'net_manager' not in list(df.columns.values):
            df['net_manager'] = filename.split('_')[0]
    df = pd.concat(df_list)
    df = df.drop(['STANDAARDDEVIATIE', 'ï»¿NETBEHEERDER', '%Defintieve aansl (NRM)', 'smartmeter_perc'], axis=1)
    names = df['net_manager'].value_counts().reset_index()['net_manager'].tolist()
    id_dict={names[i]:i for i in range(len(names))}
    df['net_manager_id'] = df['net_manager'].replace(id_dict)
    return [df, id_dict]

#Pfvtyf NaN на None
def clean_nan_values(value):
    if pd.isna(value):
        return None
    return value

#Создание таблиц и загрузка данных в mysql
def create_database(df, manager_dict):
    try:
        connection = mysql.connector.connect(
            host=secret.host,
            user=secret.user,
            password=secret.password,
            database=secret.database,
            auth_plugin='mysql_native_password'
            )
        
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS managers_mapping (
                manager_id INT PRIMARY KEY,
                manager_name VARCHAR(255) NOT NULL UNIQUE
            )
            """)
            
            for manager_name, manager_id in manager_dict.items():
                cursor.execute("""
                INSERT IGNORE INTO managers_mapping (manager_id, manager_name)
                VALUES (%s, %s)
                """, (manager_id, manager_name))

            for manager_name, manager_id in manager_dict.items():
                table_name = f"net_manager_{manager_id}"
                
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    manager_id INT NOT NULL,
                    purchase_area VARCHAR(255),
                    street VARCHAR(255),
                    zipcode_from VARCHAR(20),
                    zipcode_to VARCHAR(20),
                    city VARCHAR(255),
                    num_connections FLOAT,
                    delivery_perc FLOAT,
                    perc_of_active_connections FLOAT,
                    type_conn_perc VARCHAR(50),
                    type_of_connection VARCHAR(100),
                    annual_consume FLOAT,
                    annual_consume_lowtarif_perc FLOAT,
                    year INT,

                    FOREIGN KEY(manager_id)
                    REFERENCES managers_mapping(manager_id)
                );
                """
                cursor.execute(create_table_sql)
            
            connection.commit()
            print("БД успешно создана!")
            
            if df is not None:
                total_rows = len(df)
                print(f"Всего записей для обработки: {total_rows}")
                
                bar = IncrementalBar('Перенос данных', max=total_rows)
                
                records_by_manager = {manager_id: 0 for manager_id in manager_dict.values()}
                
                for index, row in df.iterrows():
                    manager_name = clean_nan_values(row.get('net_manager'))
                    
                    if manager_name is None:
                        bar.next()
                        continue
                    
                    manager_id = manager_dict.get(manager_name)
                    
                    if manager_id is None:
                        bar.next()
                        continue
                    
                    table_name = f"net_manager_{manager_id}"
                    
                    insert_data = (
                        manager_id,  
                        clean_nan_values(row.get('purchase_area')),
                        clean_nan_values(row.get('street')),
                        clean_nan_values(row.get('zipcode_from')),
                        clean_nan_values(row.get('zipcode_to')),
                        clean_nan_values(row.get('city')),
                        clean_nan_values(row.get('num_connections')),
                        clean_nan_values(row.get('delivery_perc')),
                        clean_nan_values(row.get('perc_of_active_connections')),
                        clean_nan_values(row.get('type_conn_perc')),
                        clean_nan_values(row.get('type_of_connection')),
                        clean_nan_values(row.get('annual_consume')),
                        clean_nan_values(row.get('annual_consume_lowtarif_perc')),
                        clean_nan_values(row.get('year'))
                    )
                    
                    insert_sql = f"""
                    INSERT INTO {table_name} (
                        manager_id, purchase_area, street, zipcode_from, zipcode_to, city,
                        num_connections, delivery_perc, perc_of_active_connections,
                        type_conn_perc, type_of_connection, annual_consume,
                        annual_consume_lowtarif_perc, year
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    try:
                        cursor.execute(insert_sql, insert_data)
                        records_by_manager[manager_id] += 1
                    except Error as e:
                        print(f"\nОшибка при вставке записи {index}: {e}")
                    
                    bar.next()
                connection.commit()

    
    except Error as e:
        print(f"\nКритическая ошибка: {e}")
        if 'connection' in locals():
            connection.rollback()
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print('Перенос данных завершённ, соединение закрыто')

        
path = './Gas/'
rename_files(path)
df, id_dict = load_dataframes(path)
create_database(df, id_dict)