# pip install python-telegram-bot
# pip install telegram

from airflow import DAG
from airflow.operators.python_operator import PythonOperator # Так как мы пишем таски в питоне
import pandahouse as ph
import pandas as pd
from io import StringIO
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns 
import io
import requests
import telegram
from airflow.decorators import dag, task


def report_mix(chat=None):
    chat_id = chat or -93865****
    
    connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}
        
    my_token = 'token'
    bot = telegram.Bot(token=my_token)
    
    query_feed = '''
    SELECT 
        user_id,
        toDate(time) date,
        sum(action = 'like') as likes,
        sum(action = 'view') as views
    FROM 
        simulator_20240120.feed_actions 
    WHERE date = today() - 1

    GROUP BY user_id, date
    
    '''
    df_main = ph.read_clickhouse(query_feed, connection=connection)
    
    likes = df_main.likes.sum()
    views = df_main.views.sum()
    ctr = (likes/views).round(2)
    DAU = df_main.user_id.nunique()
    
    msg = f"Отчет за вчерашний день:\n\nLikes = {likes}\nViews = {views}\nCTR = {ctr}\nDAU = {DAU}"
    bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='Markdown')
      
    query_feed_7 = '''
    SELECT 
        user_id,
        toDate(time) date,
        sum(action = 'like') as likes,
        sum(action = 'view') as views
    FROM 
        simulator_20240120.feed_actions 
    WHERE date > today() - 7

    GROUP BY user_id, date
    
    '''    
    df_main_7 = ph.read_clickhouse(query_feed_7, connection=connection)
    
    query_message = '''
    SELECT  user AS user_id, messages_sent,users_sent,messages_received,users_received, t1.date, os,gender,age
        FROM 
        (SELECT toDate(time) date, 
              user_id AS user,
              COUNT(user_id) as messages_sent,
              uniqExact(receiver_id) as users_sent,
              os,
              gender,
              age
        FROM simulator_20240120.message_actions  
        WHERE toDate(time) > today() - 7 
        GROUP BY user_id, os, gender, age, date) as t1
        
     FULL JOIN
        
        (SELECT toDate(time) date, 
              receiver_id as user,
              COUNT(receiver_id) messages_received,
              uniqExact(user_id) users_received
        FROM simulator_20240120.message_actions  
        WHERE toDate(time) > today() - 7 
        GROUP BY receiver_id, date) as t2 ON t1.user=t2.user
     WHERE date > today() - 7
    '''
    df_message = ph.read_clickhouse(query_message, connection=connection)
    
     
        
    df_likes = df_main_7.groupby('date', as_index=False).agg({'likes':'sum'})
    df_views = df_main_7.groupby('date', as_index=False).agg({'views':'sum'})
    df_dau = df_main_7.groupby('date', as_index=False).agg({'user_id':'nunique'})
    df_ctr = df_main_7.groupby('date', as_index=False).agg({'views': 'sum', 'likes': 'sum'})
    df_ctr['ctr'] = df_ctr.likes/df_ctr.views
    df_message_sent = df_message.groupby("date", as_index=False).agg({'messages_sent':'sum'})
    df_users_sent = df_message.groupby("date", as_index=False).agg({'users_sent':'sum'})
    df_users_received = df_message.groupby("date", as_index=False).agg({'users_received':'sum'})
    df_messages_received = df_message.groupby("date", as_index=False).agg({'messages_received':'sum'})
    df_dau_message = df_message.groupby("date", as_index=False).agg({'user_id':'nunique'})

    
    sns.set(rc={'figure.figsize':(14,6)}, style="whitegrid")
    
    msg_2 = f"Отчет в виде графиков за предыдущие 7 дней"
    bot.sendMessage(chat_id=chat_id, text=msg_2)
    
    # График для лайков
    sns.lineplot(data=df_likes, x="date", y="likes")
    sns.set(rc={'figure.figsize':(14,6)}, style="whitegrid")
    plt.title("Лайки за предыдущие 7 дней")
    plot_likes = io.BytesIO()
    plt.savefig(plot_likes)  # сохраняем график в файловый объект
    plot_likes.seek(0)  # переставили курсор в начало
    plot_likes.name = 'likes_for7days.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_likes)

    # График для просмотров
    sns.lineplot(data=df_views, x="date", y="views")
    plt.title('Просмотры за предыдущие 7 дней')
    plot_views = io.BytesIO()
    plt.savefig(plot_views)  # сохраняем график в файловый объект
    plot_views.seek(0)  # переставили курсор в начало
    plot_views.name = 'views_for7days.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_views)  

    # График для DAU
    sns.lineplot(data=df_dau, x="date", y="user_id")
    plt.title('DAU за предыдущие 7 дней')
    plot_dau = io.BytesIO()
    plt.savefig(plot_dau)  # сохраняем график в файловый объект
    plot_dau.seek(0)  # переставили курсор в начало
    plot_dau.name = 'dau_for7days.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_dau)

    # График для CTR
    sns.lineplot(data=df_ctr, x="date", y="ctr")
    plt.title('CTR за предыдущие 7 дней')
    plot_ctr = io.BytesIO()
    plt.savefig(plot_ctr)  # сохраняем график в файловый объект
    plot_ctr.seek(0)  # переставили курсор в начало
    plot_ctr.name = 'ctr_for7days.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_ctr)
    
    # График для dau_message
    sns.lineplot(data=df_dau_message, x="date", y="user_id")
    plt.title(' DAU мессаджера за предыдущие 7 дней')
    plot_dau_message = io.BytesIO()
    plt.savefig(plot_dau_message)  # сохраняем график в файловый объект
    plot_dau_message.seek(0)  # переставили курсор в начало
    plot_dau_message.name = 'dau_message_for7days.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_dau_message)
    
    # График для отправленных сообщений пользователем
    sns.lineplot(data=df_message_sent, x="date", y="messages_sent")
    plt.title('Сообщений отправлено за предыдущие 7 дней')
    plot_message_sent = io.BytesIO()
    plt.savefig(plot_message_sent)  # сохраняем график в файловый объект
    plot_message_sent.seek(0)  # переставили курсор в начало
    plot_message_sent.name = 'message_sent_for7days.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_message_sent)
    
    # График для количества пользователей отправленных сообщений
    sns.lineplot(data=df_users_sent, x="date", y="users_sent")
    plt.title('Пользователи отправленных сообщений за предыдущие 7 дней')
    plot_users_sent = io.BytesIO()
    plt.savefig(plot_users_sent)  # сохраняем график в файловый объект
    plot_users_sent.seek(0)  # переставили курсор в начало
    plot_users_sent.name = 'users_sent_for7days.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_users_sent)
    
    # График для количества пользователей получивших сообщений
    sns.lineplot(data=df_users_received, x="date", y="users_received")
    plt.title('Пользователи получившие сообщения за предыдущие 7 дней')
    plot_users_received = io.BytesIO()
    plt.savefig(plot_users_received)  # сохраняем график в файловый объект
    plot_users_received.seek(0)  # переставили курсор в начало
    plot_users_received.name = 'users_received_for7days.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_users_received)
    
     # График для количества получивших сообщений
    sns.lineplot(data=df_messages_received, x="date", y="messages_received")
    plt.title('Сообщений получено за предыдущие 7 дней')
    plot_messages_received = io.BytesIO()
    plt.savefig(plot_messages_received)  # сохраняем график в файловый объект
    plot_messages_received.seek(0)  # переставили курсор в начало
    plot_messages_received.name = 'messages_received_for7days.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_messages_received)

report_mix()    
    
default_args = {
    'owner': 't-chernov-36', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков

    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
  
    'start_date': datetime(2024, 3, 2) # Дата начала выполнения DAG
}

schedule_interval = '0 11 * * *' 

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_t_chernov_report_mix():
        
    @task
    def make_report():
        report_mix()
        
    make_report()
    
      
dag_t_chernov_report_mix = dag_t_chernov_report_mix()