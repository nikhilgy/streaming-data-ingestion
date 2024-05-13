from kafka import KafkaProducer
import requests
import json

def get_data():
    """Get Random User Data from API"""
    url = "https://randomuser.me/api/"
    result = requests.get(url)
    result = result.json()
    result = result['results'][0]

    """Format Data according to our schema"""
    user_data = {}
    user_data['full_name'] = "{} {}".format(result['name']['first'], result['name']['last'])
    user_data['gender'] = "{}".format(result['gender'])
    user_data['address'] = "{}, {}, {},{}, {}".format(result['location']['street']['number'], result['location']['street']['name'], result['location']['city'], result['location']['state'], result['location']['country']) 
    user_data['postcode'] = "{}".format(result['location']['postcode'])
    user_data['email'] = "{}".format(result['email'])
    user_data['phone'] = "{}".format(result['phone'])    

    return user_data

def stream_data(user_data):
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    producer.send('test_topic', json.dumps(user_data).encode('utf-8'))
    
    
        

def run():
    user_data = get_data()
    
    stream_data(user_data)
    
    
if __name__ == "__main__":
    
    run()