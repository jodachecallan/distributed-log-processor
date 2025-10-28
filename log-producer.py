import pika, json, time, random

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

services = ['auth', 'billing', 'inventory']

while True:
    log = {
        'service': random.choice(services),
        'level': random.choice(['INFO', 'WARN', 'ERROR']),
        'message': 'Test log message',
        'timestamp': time.time()
    }
    channel.basic_publish(
        exchange='logs',
        routing_key='',
        body=json.dumps(log),
        properties=pika.BasicProperties(delivery_mode=2)  # persistent
    )
    print("Published:", log)
    time.sleep(1)
