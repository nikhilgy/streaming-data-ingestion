from kafka import KafkaProducer
import requests
import json

import datetime
import logging
from airflow import DAG
from airflow.operators.python import (
    PythonOperator
)


def get_data():
    """Get Random User Data from API"""
    try:
        url = "https://randomuser.me/api/"
        result = requests.get(url)
        result = result.json()
        result = result['results'][0]
        
    except Exception as e:
        logging.error("Error while fetching data from API. Error is: {}".format(e))

    """Format Data according to our schema"""
    user_data = {}
    user_data['full_name'] = "{} {}".format(result['name']['first'], result['name']['last'])
    user_data['gender'] = "{}".format(result['gender'])
    user_data['address'] = "{}, {}, {},{}, {}".format(result['location']['street']['number'], result['location']['street']['name'], result['location']['city'], result['location']['state'], result['location']['country']) 
    user_data['postcode'] = "{}".format(result['location']['postcode'])
    user_data['email'] = "{}".format(result['email'])
    user_data['phone'] = "{}".format(result['phone'])    

    return user_data

def stream_data():
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    start_time = datetime.datetime.now()
    
    """Stream data for continuos 30 sec"""
    while True:
        try:
            
            user_data = get_data()
            producer.send('users_queue', json.dumps(user_data).encode('utf-8'))
            
            """Break the loop after 30 seconds of sending data"""
            if datetime.datetime.now() > start_time + datetime.timedelta(seconds=30):
                break
        
        except Exception as e:
            logging.error("Error while streaming data. Error is: {}".format(e))
            
  
  
with DAG(
    dag_id="load_user_data",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
 ) as dag:
    
    streaming_task = PythonOperator(
        task_id="load_user_data_from_api",
        python_callable=stream_data
    )
        

# def run():
#     user_data = get_data()
    
#     stream_data(user_data)
    
    
# if __name__ == "__main__":
    
#     run()