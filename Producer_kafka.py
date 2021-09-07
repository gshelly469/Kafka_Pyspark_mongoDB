from pykafka import KafkaClient
import json
import random
import threading
import time
import global_vals

#For this project, only using 2 users
users=['t1','t2']

def generate_data():
    local_host = 'localhost:9092'

    client = KafkaClient(hosts=local_host)

    print('client topics:', client.topics)
    topic = client.topics[bytes(global_vals.kafka_topic, encoding='utf-8')]
    producer = topic.get_producer()

    def work(user_number):
        while True:
            message = json.dumps({
                'timestamp': str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())),
                'uid': users[user_number],
                'heart_rate': random.randint(50, 100),
                'steps': random.randint(100, 1000)
            })
            print('message:', message)
            producer.produce(bytes(message, encoding='utf-8'))
            time.sleep(global_vals.data_produce_duration)
    
    #### Creating a thread for each users
    thread_list = [threading.Thread(target=work, args=(i,)) for i in range(len(users))]
    for thread in thread_list:
        thread.setDaemon(True)
        thread.start()

    # block it to run forever
    while True:
        time.sleep(6000)
        pass

if __name__ == '__main__':
    generate_data()
