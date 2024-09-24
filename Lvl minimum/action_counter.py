# -*- coding: utf-8 -*-
"""
Скрипт считает и сохраняет в файл CSV действия пользователей

за 7 дней до заданной даты  (не включительно)

автор: dalyuki создан: 20.09.2024
"""

import pandas as pd
from datetime import datetime, timedelta
 

 # Создание диапазона из 7 дат предшествующих текущей дате 
  
 # Читаем текущую дату

today = datetime.now().date()

 # Задаем диапазон дат

start_date = today - timedelta(days=1)

end_date = start_date + timedelta(days=6)

 # Задаем директории чтения и записи данных
 
input_dir = r"C:/Users/dalyuki.SAN-I-E14-253/Desktop/VK/Data/df_actions.csv"

dirname = r"C:/Users/dalyuki.SAN-I-E14-253/Desktop/VK/Lvl minimum/output_dir/"

output_dir = f"{dirname}{today}.csv"
 
 # Читаем выборочные даты из диапазона включаеющего start_date и end_date 
 #  и выбираем только первые 2 колонки файлa CSV
  
df = pd.read_csv(input_dir,index_col=None,sep=';')
 
df = df.loc[df["dt"].between(str(start_date), str(end_date))]
 
cols = ['email','action']

df = df[cols] 
  
 # Создаем Pivot таблицу и именуем колонки 
 
cols = ['create_count','read_count','update_count','delete_count']

df_output = df.pivot_table(index='email', columns=['action'] ,values='action',aggfunc='size',fill_value=0)
  
df_output.columns = cols
 
 # Сохраняем в файл CSV
 
df_output.to_csv(output_dir)