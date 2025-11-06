import pika, json, time, argparse

# allow overriding via command line args
parser = argparse.ArgumentParser(description="Test log consumer")
parser.add_argument('--username', default='guest', help='RabbitMQ username')
parser.add_argument('--password', default='guest', help='RabbitMQ password')
parser.add_argument('--host', default='localhost', help='RabbitMQ host')
parser.add_argument('--port', type=int, default=5672, help='RabbitMQ port')
args = parser.parse_args()

username = args.username
password = args.password
host = args.host
port = args.port

def callback(ch, method, properties, body):
    try:
        log = json.loads(body)
        print(f"[Processor] Received log from {log['service']}: {log['message']}")
        # simulate enrichment
        log['processed_at'] = time.time()
        # write to file or Elasticsearch
        with open('logs.jsonl', 'a') as f:
            f.write(json.dumps(log) + "\n")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("Error:", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
credentials = pika.PlainCredentials(username, password)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, credentials=credentials))
channel = connection.channel()

channel.queue_bind(exchange='logs', queue='logs.raw')

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='logs.raw', on_message_callback=callback)

print("Waiting for logs. To exit press CTRL+C")
channel.start_consuming()
