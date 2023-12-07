from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime, timedelta
from airflow.models import Variable
import json
import easyocr
import openai 
import re
from PIL import Image
import requests


def _create_table():
    sql = """CREATE TABLE IF NOT EXISTS marketing
    (
        text_from_picture TEXT,
        links TEXT,
        more_info TEXT
    );"""
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn', schema='airflow_db')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)


def _process_image(ti):
    url = "https://whosmailingwhat.com/blog/wp-content/uploads/2020/09/image33.jpg"
    image = Image.open(requests.get(url, stream=True).raw)
    reader = easyocr.Reader(["en"])
    result = reader.readtext(image, detail=0)
    text_from_picture = ' '.join(result)
    find_link = r'(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}'
    links = list(set(re.findall(find_link, text_from_picture)))
    if len(links) == 0:
        more_info = _get_info(links)
        ti.xcom_push("text_from_picture", text_from_picture)
        ti.xcom_push("links", "-")
        ti.xcom_push("more_info", "-")
    else:
        more_info = _get_info(links)
        ti.xcom_push("text_from_picture", text_from_picture)
        ti.xcom_push("links", ', '.join(links))
        ti.xcom_push("more_info", '; '.join(more_info))
    

def _get_info(links):
    openai.api_key = Variable.get("CHATGPT_API_KEY")
    l_info = []
    messages = [ {"role": "system", "content":  
              "You are a intelligent assistant."} ] 
    for link in links:
        message = "give two sentence about " + link 
        messages.append( 
            {"role": "user", "content": message}, 
        ) 
        chat = openai.ChatCompletion.create( 
            model="gpt-3.5-turbo", messages=messages 
        ) 
        reply = chat.choices[0].message.content 
        l_info.append(reply)
        messages.append({"role": "assistant", "content": reply}) 
    return l_info


def _insert_data(ti):
    data = [ti.xcom_pull(key="text_from_picture", task_ids="process_data"), ti.xcom_pull(key="links", task_ids="process_data"), ti.xcom_pull(key="more_info", task_ids="process_data")]
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn', schema='airflow_db')
    mysql_hook.insert_rows(table="marketing", rows=[data])



with DAG(dag_id="HW2_dag", schedule_interval="@daily", start_date=datetime(2023, 11, 20)) as dag:
    
    create_data_table = PythonOperator(
        task_id='create_data_table',
        dag=dag,
        python_callable=_create_table,
    )

    process_data = PythonOperator(
    task_id="process_data",
    dag=dag,
    python_callable=_process_image
    )

    inject_data = PythonOperator(
    task_id="inject_data",
    dag=dag,
    python_callable=_insert_data
    )




create_data_table >> process_data >> inject_data 