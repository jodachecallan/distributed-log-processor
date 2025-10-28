import pika, json

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

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)
result = channel.queue_declare(queue='logs.raw', durable=True)
channel.queue_bind(exchange='logs', queue='logs.raw')

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='logs.raw', on_message_callback=callback)

print("Waiting for logs. To exit press CTRL+C")
channel.start_consuming()
