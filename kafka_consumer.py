#!/usr/bin/env python3
"""
Kafka Consumer for Wikimedia Events
Consumes events from Kafka, calculates metrics, and generates alerts
"""

import json
import time
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import signal
import sys

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'wikimedia-events'
CONSUMER_GROUP = 'wikimedia-metrics-consumer'

DATA_DIR = Path('stream_data')
DATA_DIR.mkdir(exist_ok=True)

METRICS_FILE = DATA_DIR / 'metrics.json'
ALERTS_FILE = DATA_DIR / 'alerts.json'

# Alert threshold (bytes changed)
ALERT_THRESHOLD = 5000

# Metrics storage
metrics = defaultdict(lambda: {
    'total_edits': 0,
    'unique_editors': set(),
    'total_bytes_added': 0,
    'total_bytes_removed': 0,
    'edit_types': defaultdict(int),
    'last_update': None,
    'bot_edits': 0,
    'human_edits': 0
})

# Statistics
stats = {
    'events_consumed': 0,
    'alerts_generated': 0,
    'errors': 0,
    'start_time': datetime.now(),
    'last_save_time': time.time()
}

SAVE_INTERVAL = 30  # Save metrics every 30 seconds

def create_kafka_consumer():
    """
    Creates and returns a Kafka consumer instance.
    
    Returns:
        KafkaConsumer: Configured Kafka consumer
    """
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',  # Start from beginning if no offset
            enable_auto_commit=True,
            auto_commit_interval_ms=5000
        )
        print(f"✓ Connected to Kafka broker at {KAFKA_BROKER}")
        print(f"✓ Subscribed to topic: {KAFKA_TOPIC}")
        print(f"✓ Consumer group: {CONSUMER_GROUP}")
        return consumer
    except Exception as e:
        print(f"✗ Error connecting to Kafka: {e}")
        print("Make sure Kafka is running and the producer has created the topic.")
        sys.exit(1)

def save_metrics():
    """Saves current metrics to JSON file."""
    metrics_to_save = {}
    
    for entity, data in metrics.items():
        metrics_to_save[entity] = {
            'total_edits': data['total_edits'],
            'unique_editors_count': len(data['unique_editors']),
            'unique_editors': list(data['unique_editors'])[:100],  # Limit to 100 for file size
            'total_bytes_added': data['total_bytes_added'],
            'total_bytes_removed': data['total_bytes_removed'],
            'edit_types': dict(data['edit_types']),
            'last_update': data['last_update'],
            'bot_edits': data['bot_edits'],
            'human_edits': data['human_edits']
        }
    
    output = {
        'timestamp': datetime.now().isoformat(),
        'metrics': metrics_to_save,
        'total_events_processed': stats['events_consumed'],
        'alerts_generated': stats['alerts_generated']
    }
    
    with open(METRICS_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

def save_alert(entity, event_data):
    """
    Saves an alert to file.
    
    Args:
        entity: Entity name
        event_data: Event data dictionary
    """
    alert = {
        'timestamp': datetime.now().isoformat(),
        'entity': entity,
        'user': event_data.get('user', 'Unknown'),
        'edit_type': event_data.get('type', 'Unknown'),
        'bytes_changed': event_data.get('bytes_changed', 0),
        'comment': event_data.get('comment', ''),
        'title': event_data.get('title', ''),
        'revision_id': event_data.get('revision', {}).get('new', 0),
        'bot': event_data.get('bot', False)
    }
    
    # Load existing alerts
    alerts = []
    if ALERTS_FILE.exists():
        try:
            with open(ALERTS_FILE, 'r', encoding='utf-8') as f:
                alerts = json.load(f)
        except:
            alerts = []
    
    alerts.append(alert)
    
    # Save with new alert
    with open(ALERTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(alerts, f, indent=2, ensure_ascii=False)
    
    stats['alerts_generated'] += 1
    
    print(f"\n{'='*70}")
    print(f"🚨 ALERT: Major edit detected on {entity}")
    print(f"{'='*70}")
    print(f"User: {alert['user']}")
    print(f"Change: {alert['bytes_changed']:+d} bytes")
    print(f"Bot: {'Yes' if alert['bot'] else 'No'}")
    print(f"Comment: {alert['comment'][:100]}")
    print(f"{'='*70}\n")

def process_event(event_data):
    """
    Processes a single event from Kafka.
    
    Args:
        event_data: Event data dictionary
    """
    try:
        entity = event_data.get('entity', 'unknown')
        
        # Update metrics
        metrics[entity]['total_edits'] += 1
        metrics[entity]['unique_editors'].add(event_data.get('user', 'Anonymous'))
        metrics[entity]['last_update'] = event_data.get('timestamp', datetime.now().isoformat())
        
        # Calculate bytes changed
        bytes_changed = event_data.get('bytes_changed', 0)
        
        if bytes_changed > 0:
            metrics[entity]['total_bytes_added'] += bytes_changed
        else:
            metrics[entity]['total_bytes_removed'] += abs(bytes_changed)
        
        # Track edit type
        edit_type = event_data.get('type', 'unknown')
        metrics[entity]['edit_types'][edit_type] += 1
        
        # Track bot vs human edits
        if event_data.get('bot', False):
            metrics[entity]['bot_edits'] += 1
        else:
            metrics[entity]['human_edits'] += 1
        
        stats['events_consumed'] += 1
        
        # Print progress
        if stats['events_consumed'] % 10 == 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"Processed {stats['events_consumed']} events")
        
        # Check for alert
        if abs(bytes_changed) > ALERT_THRESHOLD:
            save_alert(entity, event_data)
        
        # Periodic save
        if time.time() - stats['last_save_time'] > SAVE_INTERVAL:
            save_metrics()
            stats['last_save_time'] = time.time()
            print(f"\n✓ Metrics saved ({stats['events_consumed']} events processed)\n")
        
    except Exception as e:
        stats['errors'] += 1
        print(f"✗ Error processing event: {e}")

def consume_events(consumer):
    """
    Main consumer loop.
    
    Args:
        consumer: Kafka consumer instance
    """
    print("\n" + "=" * 70)
    print("KAFKA CONSUMER - Processing Wikimedia Events")
    print("=" * 70)
    print(f"\nAlert threshold: {ALERT_THRESHOLD} bytes")
    print(f"Output files:")
    print(f"  - Metrics: {METRICS_FILE}")
    print(f"  - Alerts: {ALERTS_FILE}")
    print("\nConsuming events... (Ctrl+C to stop)\n")
    
    try:
        for message in consumer:
            event_data = message.value
            process_event(event_data)
            
    except KeyboardInterrupt:
        print("\n\nStopping consumer...")
    except Exception as e:
        print(f"\n✗ Consumer error: {e}")
    finally:
        # Final save
        save_metrics()
        print("\n✓ Final metrics saved")

def print_stats():
    """Prints statistics."""
    runtime = (datetime.now() - stats['start_time']).total_seconds()
    
    print("\n" + "=" * 70)
    print("CONSUMER STATISTICS")
    print("=" * 70)
    print(f"Runtime: {runtime:.1f} seconds")
    print(f"Events consumed: {stats['events_consumed']:,}")
    print(f"Alerts generated: {stats['alerts_generated']:,}")
    print(f"Errors: {stats['errors']:,}")
    if runtime > 0:
        print(f"Events per second: {stats['events_consumed']/runtime:.2f}")
    
    print("\n" + "=" * 70)
    print("METRICS BY ENTITY")
    print("=" * 70)
    
    for entity, data in sorted(metrics.items()):
        print(f"\n{entity.replace('_', ' ')}:")
        print(f"  Total edits: {data['total_edits']}")
        print(f"  Unique editors: {len(data['unique_editors'])}")
        print(f"  Bytes added: {data['total_bytes_added']:,}")
        print(f"  Bytes removed: {data['total_bytes_removed']:,}")
        print(f"  Bot edits: {data['bot_edits']}")
        print(f"  Human edits: {data['human_edits']}")
        print(f"  Edit types: {dict(data['edit_types'])}")
    
    print("\n" + "=" * 70)

def signal_handler(sig, frame):
    """Handles graceful shutdown."""
    print('\n\nShutting down consumer...')
    save_metrics()
    print_stats()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def display_current_metrics():
    """Displays current metrics from file."""
    if not METRICS_FILE.exists():
        print("No metrics file found. Run the consumer first.")
        return
    
    with open(METRICS_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print("\n" + "=" * 70)
    print("CURRENT METRICS")
    print("=" * 70)
    print(f"Last updated: {data['timestamp']}")
    print(f"Total events processed: {data['total_events_processed']:,}")
    print(f"Alerts generated: {data['alerts_generated']:,}")
    
    for entity, metrics_data in data['metrics'].items():
        print(f"\n{entity.replace('_', ' ')}:")
        print(f"  Total edits: {metrics_data['total_edits']}")
        print(f"  Unique editors: {metrics_data['unique_editors_count']}")
        print(f"  Bytes added: {metrics_data['total_bytes_added']:,}")
        print(f"  Bytes removed: {metrics_data['total_bytes_removed']:,}")
        print(f"  Bot edits: {metrics_data.get('bot_edits', 0)}")
        print(f"  Human edits: {metrics_data.get('human_edits', 0)}")
        print(f"  Edit types: {metrics_data['edit_types']}")
    
    if ALERTS_FILE.exists():
        with open(ALERTS_FILE, 'r', encoding='utf-8') as f:
            alerts = json.load(f)
        
        print(f"\n" + "=" * 70)
        print(f"ALERTS ({len(alerts)} total)")
        print("=" * 70)
        
        for alert in alerts[-5:]:  # Show last 5
            print(f"\n[{alert['timestamp']}]")
            print(f"  Entity: {alert['entity']}")
            print(f"  User: {alert['user']}")
            print(f"  Change: {alert['bytes_changed']:+d} bytes")
            print(f"  Bot: {'Yes' if alert.get('bot', False) else 'No'}")
    
    print("\n" + "=" * 70)

def main():
    """Main function."""
    import sys
    
    # Check for stats mode
    if len(sys.argv) > 1 and sys.argv[1] == 'stats':
        display_current_metrics()
        return
    
    print("=" * 70)
    print("WIKIMEDIA KAFKA CONSUMER")
    print("=" * 70)
    print()
    print("Prerequisites:")
    print("  1. Kafka must be running")
    print("  2. Producer must be publishing events")
    print()
    
    # Create consumer
    consumer = create_kafka_consumer()
    
    try:
        # Start consuming
        consume_events(consumer)
    finally:
        # Cleanup
        print("\nClosing consumer...")
        consumer.close()
        print_stats()
        print("✓ Consumer closed")

if __name__ == '__main__':
    main()
