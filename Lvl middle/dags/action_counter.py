# -*- coding: utf-8 -*-
"""
Скрипт генерирует датасет заданного размера, 

содержащий e-mail, действие и дату действия пользователя,

на основе данных, полученных из  CSV файла. Запуск скрипта производится через

Apache Airflow в 7-00  каждого дня

автор: dalyuki создан: 20.09.2024
"""


# 1. Открыть CSV файл в заданной директории
# 2. Сгенерировать DataFrame из 5 колонок (email, CREATE, READ, UPDATE, DELETE)  
# - с подсчетом количества действий каждого пользователя (email)
# 3. Сохраните DataFrame в csv-файл, первая строка - название колонок.


# Импорт библиотек и модулей
 
from datetime import datetime, timedelta 
from pyspark.sql import SparkSession

# Создадим сессию в Spark
spark = SparkSession.builder.appName("Action_counter_middle").getOrCreate()

spark = (SparkSession
 .builder
 .appName('Action_counter_middle')
 .enableHiveSupport()
 .getOrCreate())

 
 # Создание диапазона из 7 дат предшествующих текущей дате 
  
 # Читаем текущую дату

today = datetime.now().date()

 # Задаем диапазон дат

start_date = today - timedelta(days=1)

end_date = start_date + timedelta(days=6)

# Директория с исходным файлом CSV

input_dir = r"C:/Users/dalyuki.SAN-I-E14-253/Desktop/VK/Data/df_actions.csv"
 
# Директория для сохранения нового файла CSV

dirname = r"C:/Users/dalyuki.SAN-I-E14-253/Desktop/VK/Lvl middle/output_dir/"
 
output_dir = f"{dirname}{today}.csv"


# Прочитаем исходный файл CSV  в dataftame

input_df = spark.read.option("delimiter", ";").option("header", "true").csv(input_dir)

# Укажем даты для фильтрации действий из датафрейма

dates = (str(start_date), str(end_date))
 

# Отфильтруем строки в требуемом диапазоне дат

input_df.filter(input_df.dt.between(*dates)).show()
  
# Вывод на экран

input_df.show()
 

# Создадим Pivot таблицу


pivot_table = input_df.groupBy("email").pivot("action").count().fillna(0)


pivot_table.show()
  
# Переименуем колонки

pivot_table = pivot_table.withColumnsRenamed({'CREATE': 'create_count', 
                           'DELETE': 'delete_count',
                           'READ': 'read_count',
                           'UPDATE': 'update_count'})
 
pivot_table.show()

# Сохраним датафрейм как CSV

pivot_table.toPandas().to_csv(output_dir, sep="\t" , header=True, index=False)

 