# clickstream_producer.py
"""
Real-time Clickstream Event Producer for streammart Demo
Produces realistic user behavior events to Kafka

Usage:
    python clickstream_producer.py --duration 15 --rate 10
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import random
import argparse
from datetime import datetime
from generate_data import generate_user_session, PRODUCTS

try:
    from config import get_kafka_config, KAFKA_TOPIC_CLICKSTREAM
except ImportError:
    print("⚠️  config.py not found. Using default settings.")
    KAFKA_TOPIC_CLICKSTREAM = 'streammart_clickstream'
    def get_kafka_config():
        return {
            'bootstrap_servers': 'localhost:9092',
            'value_serializer': lambda v: json.dumps(v).encode('utf-8')
        }

class ClickstreamProducer:
    def __init__(self, topic=KAFKA_TOPIC_CLICKSTREAM):
        self.topic = topic
        self.producer = None
        self.total_events = 0
        self.total_sessions = 0
        self.errors = 0
        
    def connect(self):
        """Connect to Kafka"""
        try:
            kafka_config = get_kafka_config()
            kafka_config['value_serializer'] = lambda v: json.dumps(v).encode('utf-8')
            
            self.producer = KafkaProducer(**kafka_config)
            print(f"✅ Connected to Kafka")
            print(f"📤 Publishing to topic: {self.topic}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def send_event(self, event):
        """Send a single event to Kafka"""
        try:
            future = self.producer.send(self.topic, value=event)
            # Wait for acknowledgment (optional - comment out for higher throughput)
            record_metadata = future.get(timeout=10)
            self.total_events += 1
            return True
        except KafkaError as e:
            self.errors += 1
            print(f"❌ Error sending event: {e}")
            return False
    
    def produce_session(self, user_id=None):
        """Produce a full user session"""
        session_events = generate_user_session(user_id=user_id)
        
        for event in session_events:
            # Update timestamp to now
            event['event_timestamp'] = datetime.now().isoformat()
            
            # Send to Kafka
            if self.send_event(event):
                # Small delay between events in a session
                time.sleep(random.uniform(0.5, 2))
        
        self.total_sessions += 1
        return len(session_events)
    
    def start_producing(self, duration_minutes=15, events_per_second=10):
        """
        Start producing events
        
        Args:
            duration_minutes: How long to produce (minutes)
            events_per_second: Target rate
        """
        if not self.connect():
            return
        
        print("\n" + "=" * 60)
        print("🚀 Starting Clickstream Producer")
        print("=" * 60)
        print(f"⏱️  Duration: {duration_minutes} minutes")
        print(f"📊 Target rate: {events_per_second} events/second")
        print(f"🎯 Estimated total: ~{events_per_second * duration_minutes * 60} events")
        print("=" * 60)
        print("\nPress Ctrl+C to stop\n")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        try:
            while time.time() < end_time:
                loop_start = time.time()
                
                # Produce events
                events_this_second = 0
                target_events = events_per_second
                
                while events_this_second < target_events:
                    self.produce_session()
                    events_this_second += 1
                    
                    # Status update every 10 seconds
                    if self.total_events % (events_per_second * 10) == 0:
                        elapsed = time.time() - start_time
                        rate = self.total_events / elapsed if elapsed > 0 else 0
                        remaining = (end_time - time.time()) / 60
                        
                        print(f"📊 Events: {self.total_events:,} | "
                              f"Sessions: {self.total_sessions:,} | "
                              f"Rate: {rate:.1f}/s | "
                              f"Remaining: {remaining:.1f}m | "
                              f"Errors: {self.errors}")
                
                # Sleep to maintain rate
                loop_time = time.time() - loop_start
                sleep_time = max(0, 1 - loop_time)
                time.sleep(sleep_time)
        
        except KeyboardInterrupt:
            print("\n\n⚠️  Stopping producer (Ctrl+C detected)...")
        
        finally:
            self.stop()
    
    def stop(self):
        """Stop producer and print statistics"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        print("\n" + "=" * 60)
        print("📊 Final Statistics")
        print("=" * 60)
        print(f"Total Events Sent: {self.total_events:,}")
        print(f"Total Sessions: {self.total_sessions:,}")
        print(f"Errors: {self.errors}")
        print("=" * 60)

# ==============================================
# CLI
# ==============================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Produce clickstream events to Kafka')
    parser.add_argument('--duration', type=int, default=15,
                       help='Duration in minutes (default: 15)')
    parser.add_argument('--rate', type=int, default=10,
                       help='Events per second (default: 10)')
    parser.add_argument('--topic', type=str, default=None,
                       help='Kafka topic (default from config)')
    
    args = parser.parse_args()
    
    # Create and start producer
    topic = args.topic if args.topic else KAFKA_TOPIC_CLICKSTREAM
    producer = ClickstreamProducer(topic=topic)
    producer.start_producing(
        duration_minutes=args.duration,
        events_per_second=args.rate
    )

