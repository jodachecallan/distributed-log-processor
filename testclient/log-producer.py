import pika, json, time, random, sys, argparse

# allow overriding via command line args
parser = argparse.ArgumentParser(description="Test log producer")
parser.add_argument('--username', default='guest', help='RabbitMQ username')
parser.add_argument('--password', default='guest', help='RabbitMQ password')
parser.add_argument('--host', default='localhost', help='RabbitMQ host')
parser.add_argument('--port', type=int, default=5672, help='RabbitMQ port')
args = parser.parse_args()

username = args.username
password = args.password
host = args.host
port = args.port

def connect_with_retry(max_retries=None, initial_delay=1, factor=2, max_delay=30):
    credentials = pika.PlainCredentials(username, password)
    params = pika.ConnectionParameters(host=host, port=port, credentials=credentials)
    attempt = 0
    delay = initial_delay
    while True:
        try:
            return pika.BlockingConnection(params)
        except Exception as e:
            attempt += 1
            if max_retries is not None and attempt > max_retries:
                print(f"Failed to connect after {attempt-1} attempts: {e}", file=sys.stderr)
                raise
            print(f"Connection attempt {attempt} failed: {e}; retrying in {delay}s...", file=sys.stderr)
            time.sleep(delay)
            delay = min(delay * factor, max_delay)

connection = connect_with_retry()
channel = connection.channel()

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
