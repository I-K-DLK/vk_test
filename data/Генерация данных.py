# -*- coding: utf-8 -*-
"""
Скрипт генерирует датасет заданного размера, 

содержащий e-mail, действие и дату действия пользователей 

автор: dalyuki создан: 20.09.2024
"""

import pandas as pd
import random 
from datetime import datetime, timedelta
from faker import Faker 
fake = Faker('ru_RU')


# Создание диапазона из 10 дат предшествующих текущей дате 
# (для тестирования отсеивания нерелевантных)

# Читаем текущую дату

today = datetime.now().date()

# Задаем диапазон дат

start_date = today - timedelta(days=1)

end_date = start_date + timedelta(days=9)
 
 
# Директория для сохранения данных в  CSV 
 
dir_name = r"C:/Users/dalyuki.SAN-I-E14-253/Desktop/VK/Data/df_actions.csv"
 

# Создадим списoк email пользователей

email_list = []

# Зададим списoк действий пользователей 

action_types = [
    "CREATE",
    "READ",
    "UPDATE",
    "DELETE",
]


# Генерируем списки email
    
for _ in range(100):
    
      email_list.append(fake.ascii_free_email())
       
 
# Генерируем dataframe для 1000 действий пользователей
 
df = pd.DataFrame(
    [
        {   
            "action": random.choice(action_types),
            "email": random.choice(email_list),      
            "dt": fake.date_between(start_date, end_date)
        }
        for r in range(1000) 
    ]
)
 
# Сохраняем в целевую директорию

df.to_csv(dir_name,index=0,sep=';')