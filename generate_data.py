from faker import Faker
from kafka import KafkaProducer
import random
import time
import json 

faker = Faker(['en_KE'])  #kenya Locale for realistic data 
Faker.seed(42)
random.seed(42)

#Generate the ecommerce data 
def generate_data():
    data={}
    data['user_id']=random.getrandbits(32)
    data['user_type']=random.choice(['click','view','purchase'])
    data['country']=faker.country_code()
    data['city']=faker.city()
    data['device']=random.choice(['mobile','desktop','tablet'])
    data['product_id']=random.getrandbits(32)
    data['price']=round(random.uniform(5,500),2)
    data['quantity']=random.randint(1,100)
    data['timestamp']=round(time.time())

    return data

if __name__=='__main__':
    fake=Faker()
    topic_name='ecommerece_data'
    producer=KafkaProducer(
        bootstrap_servers=['localhost:29092' , 'localhost:39092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    count = 0

    while count < 500:
        rec = generate_data()
        producer.send(topic_name,value=rec)
        count +=1
        print(f'Record[{count}]:{rec}')
        producer.flush()

