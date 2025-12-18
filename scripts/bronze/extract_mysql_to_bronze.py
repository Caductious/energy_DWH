import os

import mysql.connector
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from progress.bar import IncrementalBar
from dotenv import load_dotenv

load_dotenv()

mysql_engine = create_engine(f"mysql+mysqlconnector://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}/{os.getenv('MYSQL_DATABASE')}?auth_plugin=mysql_native_password")
postgres_engine = create_engine(f"postgresql+psycopg2://{os.getenv('PSQL_USER')}:{os.getenv('PSQL_PASSWORD')}@{os.getenv('PSQL_HOST')}:5432/{os.getenv('PSQL_DATABASE')}")

with mysql_engine.connect() as conn:
    tables = pd.read_sql("SHOW TABLES", conn)['Tables_in_' + os.getenv('MYSQL_DATABASE')].tolist()
print(f"Найдено таблиц в MySQL: {len(tables)}")

bar = IncrementalBar('Перенос таблиц', max=len(tables))

for table in tables:
    table_name = table.decode('utf-8')
    df = pd.read_sql(f"SELECT * FROM `{table_name}`", mysql_engine)    
    df.to_sql(
        name=f"mysql_{table_name}",
        con=postgres_engine,
        schema='bronze',
        if_exists='replace',
        index=False
    )
    bar.next()
bar.finish()
print("Готово!")