# Реализация хранилища данных для системы мониторинга энергопотребления
## Курсовая работа
### Выполнил Логинов Пётр Константинович в 2025 году
Финансовый университет при правительстве РФ. Группа ИД22-4. 
Цель работы - создание хранилища данных для систем мониторинга энергопотребления.
В качестве исходных данных я взял [датасет о потреблении энергии в Нидерландах](https://www.kaggle.com/datasets/lucabasa/dutch-energy)


## Для того, чтоб развернуть проект у себя на ПК:
1. скопируйте репозиторий при помощи git clone.
2. скачайте датасет.
```
curl -L -o ./dutch-energy.zip\ https://www.kaggle.com/api/v1/datasets/download/lucabasa/dutch-energy
```
3. Распакуйте датасет в папку проекта.
4. В mysql создайте базу данных  я газа.
5. создайте файл ```.env``` с содержимым вида:
```python
MYSQL_USER=имя_пользователя
MYSQL_PASSWORD=ваш_пароль
MYSQL_HOST=localhost
MYSQL_DATABASE=gas_db

PSQL_USER=etl_user
PSQL_PASSWORD=your_password
PSQL_HOST=localhost
PSQL_DATABASE=energy_data_vault
```
6. Создайте виртуальную среду и установите библиотеки:
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
7. Запустите файл ```preparation/populate_gas.py```  для заполнения БД mysql с данными о газе.
8. Запустите файл ```preparation/fix_electricity_names.py``` для редактирования неправильно введенных имён в файлах об электричестве.
9. Для создания БД используйте запустите скрипт ```preparation/create_DB.sql``` откройте его в текстовом редакторе и измените пароль для пользователя etl_user в 10 строке, после этого запустите скрипт.
```
psql -h localhost -U postgres -f preparation/create_DB.sql
```
10. Запустите скрипт для создания таблиц схемы звезда:
```
psql -h localhost -U postgres -f preparation/create_star.sql
```
11. Запустите скрипты по уровням звезды от бронзового до золотого