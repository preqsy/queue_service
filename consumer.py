import pika
import json
import os
from dotenv import load_dotenv, find_dotenv

env = load_dotenv(find_dotenv("RABBITMQURL"))

BROKER_URL = "amqp://guest:guest@localhost:5673"

# # env = os.getenv("RABBITMQURL")

print(env)


def consume_message(queue_name: str, callback):
    connection = pika.BlockingConnection(pika.URLParameters(BROKER_URL))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    def on_message(ch, method, properties, body):

        message = json.loads(body)

        callback(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue_name, on_message_callback=on_message)
    print(f"Listening to queue '{queue_name}'...")
    channel.start_consuming()


def callback(message):
    print(type(message))
    print(message)


consume_message(queue_name="new.user.createdd", callback=callback)
