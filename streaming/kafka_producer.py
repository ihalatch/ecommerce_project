import json
import time
from kafka import KafkaProducer
from loguru import logger
from config import KAFKA_BROKER, KAFKA_TOPIC

# Read customers from JSON file
def load_customers(filename='customers.json'):
    with open(filename, 'r') as f:
        return json.load(f)

def main():
    logger.info('Starting Kafka producer...')
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    customers = load_customers()
    logger.info(f'Loaded {len(customers)} customers from file.')
    for idx, customer in enumerate(customers, 1):
        try:
            producer.send(KAFKA_TOPIC, customer)
            logger.info(f'Sent customer {customer["customer_id"]} to topic.')
            time.sleep(0.1)  # Simulate real-time streaming
        except Exception as e:
            logger.error(f'Error sending customer {customer["customer_id"]}: {e}')
    producer.flush()
    logger.info('All customers sent. Producer finished.')

if __name__ == '__main__':
    main() 