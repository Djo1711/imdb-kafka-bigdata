#!/usr/bin/env python3
"""
Kafka Producer for Wikimedia EventStreams
Connects to Wikimedia EventStreams and publishes events to Kafka topic
"""

import json
import requests
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import signal
import sys
from datetime import datetime

# Configuration
STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'wikimedia-events'

# Entities to monitor (Wikipedia page titles)
MONITORED_ENTITIES = [
    'The_Shawshank_Redemption',
    'Christopher_Nolan',
    'Leonardo_DiCaprio',
    'Marvel_Cinematic_Universe',
    'Academy_Awards'
]

# Statistics
stats = {
    'total_events': 0,
    'published_events': 0,
    'errors': 0,
    'start_time': datetime.now()
}

def create_kafka_producer():
    """
    Creates and returns a Kafka producer instance.
    
    Returns:
        KafkaProducer: Configured Kafka producer
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        print(f"✓ Connected to Kafka broker at {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"✗ Error connecting to Kafka: {e}")
        print("Make sure Kafka is running. Start it with:")
        print("  docker-compose up -d")
        sys.exit(1)

def is_monitored_entity(title):
    """
    Checks if a Wikipedia page title matches any monitored entity.
    
    Args:
        title: Wikipedia page title
    
    Returns:
        str: Matched entity name or None
    """
    normalized_title = title.replace(' ', '_')
    for entity in MONITORED_ENTITIES:
        if entity in normalized_title:
            return entity
    return None

def publish_to_kafka(producer, event_data):
    """
    Publishes an event to Kafka topic.
    
    Args:
        producer: Kafka producer instance
        event_data: Event data dictionary
    """
    try:
        # Extract entity name for use as key (enables partitioning)
        entity = event_data.get('entity', 'unknown')
        
        # Publish to Kafka
        future = producer.send(
            KAFKA_TOPIC,
            key=entity,
            value=event_data
        )
        
        # Wait for confirmation (with timeout)
        record_metadata = future.get(timeout=10)
        
        stats['published_events'] += 1
        
        if stats['published_events'] % 10 == 0:
            print(f"✓ Published {stats['published_events']} events "
                  f"(partition: {record_metadata.partition}, "
                  f"offset: {record_metadata.offset})")
        
    except KafkaError as e:
        stats['errors'] += 1
        print(f"✗ Kafka error: {e}")
    except Exception as e:
        stats['errors'] += 1
        print(f"✗ Error publishing event: {e}")

def process_stream_event(event_data, producer):
    """
    Processes a single event from Wikimedia stream.
    
    Args:
        event_data: Raw event data dictionary
        producer: Kafka producer instance
    """
    stats['total_events'] += 1
    
    try:
        title = event_data.get('title', '')
        entity = is_monitored_entity(title)
        
        if entity:
            # Create enriched event
            enriched_event = {
                'entity': entity,
                'timestamp': event_data.get('timestamp', datetime.now().isoformat()),
                'user': event_data.get('user', 'Anonymous'),
                'type': event_data.get('type', 'unknown'),
                'title': title,
                'comment': event_data.get('comment', ''),
                'length': {
                    'old': event_data.get('length', {}).get('old', 0),
                    'new': event_data.get('length', {}).get('new', 0)
                },
                'revision': event_data.get('revision', {}),
                'server_name': event_data.get('server_name', ''),
                'bot': event_data.get('bot', False)
            }
            
            # Calculate bytes changed
            bytes_changed = enriched_event['length']['new'] - enriched_event['length']['old']
            enriched_event['bytes_changed'] = bytes_changed
            
            # Publish to Kafka
            publish_to_kafka(producer, enriched_event)
            
            # Print event info
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Event captured")
            print(f"  Entity: {entity}")
            print(f"  User: {enriched_event['user']}")
            print(f"  Type: {enriched_event['type']}")
            print(f"  Change: {bytes_changed:+d} bytes")
            
    except Exception as e:
        stats['errors'] += 1
        print(f"✗ Error processing event: {e}")

def connect_to_stream(producer):
    """
    Connects to Wikimedia EventStreams and processes events.
    
    Args:
        producer: Kafka producer instance
    """
    print("\n" + "=" * 70)
    print("KAFKA PRODUCER - Wikimedia EventStreams")
    print("=" * 70)
    print(f"\nMonitored entities:")
    for i, entity in enumerate(MONITORED_ENTITIES, 1):
        print(f"  {i}. {entity.replace('_', ' ')}")
    
    print(f"\nKafka topic: {KAFKA_TOPIC}")
    print("Connecting to Wikimedia stream...")
    print("=" * 70)
    print()
    
    while True:
        try:
            response = requests.get(STREAM_URL, stream=True, timeout=60)
            
            if response.status_code == 200:
                print("✓ Connected to Wikimedia EventStreams")
                print("Publishing events to Kafka... (Ctrl+C to stop)\n")
                
                for line in response.iter_lines():
                    if line:
                        line = line.decode('utf-8')
                        
                        # Events are prefixed with "data: "
                        if line.startswith('data: '):
                            try:
                                event_json = line[6:]  # Remove "data: " prefix
                                event_data = json.loads(event_json)
                                process_stream_event(event_data, producer)
                            except json.JSONDecodeError:
                                continue
            else:
                print(f"✗ Connection error: {response.status_code}")
                time.sleep(5)
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Connection error: {e}")
            print("Reconnecting in 5 seconds...")
            time.sleep(5)
        except KeyboardInterrupt:
            break

def print_stats():
    """Prints statistics before exit."""
    runtime = (datetime.now() - stats['start_time']).total_seconds()
    
    print("\n\n" + "=" * 70)
    print("PRODUCER STATISTICS")
    print("=" * 70)
    print(f"Runtime: {runtime:.1f} seconds")
    print(f"Total events processed: {stats['total_events']:,}")
    print(f"Events published to Kafka: {stats['published_events']:,}")
    print(f"Errors: {stats['errors']:,}")
    if runtime > 0:
        print(f"Events per second: {stats['published_events']/runtime:.2f}")
    print("=" * 70)

def signal_handler(sig, frame):
    """Handles graceful shutdown."""
    print('\n\nShutting down producer...')
    print_stats()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def main():
    """Main function."""
    print("=" * 70)
    print("WIKIMEDIA TO KAFKA PRODUCER")
    print("=" * 70)
    print()
    print("Prerequisites:")
    print("  1. Kafka must be running (docker-compose up -d)")
    print("  2. Topic 'wikimedia-events' will be created automatically")
    print()
    
    # Create Kafka producer
    producer = create_kafka_producer()
    
    try:
        # Connect to stream and start publishing
        connect_to_stream(producer)
    finally:
        # Cleanup
        print("\nClosing Kafka producer...")
        producer.flush()
        producer.close()
        print_stats()
        print("✓ Producer closed")

if __name__ == '__main__':
    main()
