# ==============================================
# TRANSACTION PRODUCER
# ==============================================

# transaction_producer.py
"""
Real-time Transaction Event Producer
Produces occasional transaction events (lower frequency than clickstream)

Usage:
    python transaction_producer.py --duration 15 --rate 1
"""

from kafka import KafkaProducer
import json
import time
import argparse
from datetime import datetime
from generate_data import generate_transaction

try:
    from config import get_kafka_config, KAFKA_TOPIC_TRANSACTIONS
except ImportError:
    KAFKA_TOPIC_TRANSACTIONS = 'streammart_transactions'
    def get_kafka_config():
        return {'bootstrap_servers': 'localhost:9092'}

class TransactionProducer:
    def __init__(self, topic=KAFKA_TOPIC_TRANSACTIONS):
        self.topic = topic
        self.producer = None
        self.total_transactions = 0
        
    def connect(self):
        """Connect to Kafka"""
        try:
            kafka_config = get_kafka_config()
            kafka_config['value_serializer'] = lambda v: json.dumps(v).encode('utf-8')
            self.producer = KafkaProducer(**kafka_config)
            print(f"✅ Connected to Kafka (Transactions)")
            return True
        except Exception as e:
            print(f"❌ Failed to connect: {e}")
            return False
    
    def produce_transaction(self):
        """Produce a single transaction"""
        transaction = generate_transaction()
        transaction['transaction_timestamp'] = datetime.now().isoformat()
        
        try:
            self.producer.send(self.topic, value=transaction)
            self.total_transactions += 1
            return True
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def start_producing(self, duration_minutes=15, rate_per_minute=1):
        """Start producing transactions"""
        if not self.connect():
            return
        
        print(f"\n🚀 Transaction Producer Started")
        print(f"   Duration: {duration_minutes} minutes")
        print(f"   Rate: {rate_per_minute} transactions/minute")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        try:
            while time.time() < end_time:
                self.produce_transaction()
                time.sleep(60 / rate_per_minute)  # Sleep between transactions
                
                if self.total_transactions % 10 == 0:
                    print(f"   Transactions sent: {self.total_transactions}")
        
        except KeyboardInterrupt:
            print("\n⚠️  Stopping...")
        
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            print(f"\n✅ Total Transactions: {self.total_transactions}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--duration', type=int, default=15)
    parser.add_argument('--rate', type=int, default=1)
    args = parser.parse_args()
    
    producer = TransactionProducer()
    producer.start_producing(args.duration, args.rate)

