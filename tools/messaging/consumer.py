#!/usr/bin/env python3
"""
Consumer for llama-messaging tool.

This script receives inference responses from the llama-messaging tool via RabbitMQ
without sending prompts. Useful for load testing and monitoring responses.

Usage:
    python consumer.py [--host HOST] [--port PORT] [--count COUNT] [--timeout TIMEOUT]
"""

import argparse
import pika
import sys
import time
import signal
from typing import Optional


class LlamaMessagingConsumer:
    def __init__(self, host: str = 'localhost', port: int = 5672, 
                 username: str = 'guest', password: str = 'guest',
                 vhost: str = '/'):
        """Initialize the consumer with RabbitMQ connection parameters."""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        
        # Queue and exchange names to match messaging.cpp
        self.response_queue = 'inference_response'
        self.exchange = 'amq.direct'
        self.response_routing_key = 'inference.response'
        
        self.connection = None
        self.channel = None
        self.response_count = 0
        self.running = False

    def connect(self) -> bool:
        """Connect to RabbitMQ and set up channels."""
        try:
            # Create connection
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.vhost,
                credentials=credentials
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declare queue and exchange
            self.channel.queue_declare(queue=self.response_queue, durable=True)
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=True)
            
            # Bind queue to exchange with routing key
            self.channel.queue_bind(
                exchange=self.exchange, 
                queue=self.response_queue, 
                routing_key=self.response_routing_key
            )
            
            print(f"Connected to RabbitMQ at {self.host}:{self.port}")
            print(f"Response queue: {self.response_queue}")
            return True
            
        except Exception as e:
            print(f"Failed to connect to RabbitMQ: {e}")
            return False

    def disconnect(self):
        """Close the RabbitMQ connection."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def response_callback(self, ch, method, properties, body):
        """Callback function to handle incoming responses."""
        try:
            # Response is plain text, not JSON
            response_data = body.decode('utf-8')
            self.response_count += 1
            
            # Extract message ID if available
            message_id = properties.message_id if properties.message_id else "unknown"
            
            print(f"\n{'='*60}")
            print(f"RESPONSE #{self.response_count}")
            print(f"Message ID: {message_id}")
            print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")
            print(response_data)
            print(f"{'='*60}")
            
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Failed to process response: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self, max_responses: Optional[int] = None, timeout: Optional[float] = None):
        """Start consuming responses from the queue."""
        if not self.channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        print(f"Starting to consume responses...")
        if max_responses:
            print(f"Will stop after receiving {max_responses} responses")
        if timeout:
            print(f"Will stop after {timeout} seconds")
        
        # Set up consumer
        self.channel.basic_consume(
            queue=self.response_queue,
            on_message_callback=self.response_callback,
            auto_ack=False
        )
        
        self.running = True
        start_time = time.time()
        
        try:
            while self.running:
                # Check if we've reached the maximum number of responses
                if max_responses and self.response_count >= max_responses:
                    print(f"\nReached maximum responses ({max_responses}), stopping...")
                    break
                
                # Check if we've exceeded the timeout
                if timeout and (time.time() - start_time) >= timeout:
                    print(f"\nTimeout reached ({timeout}s), stopping...")
                    break
                
                # Process events with a short timeout to allow for graceful shutdown
                try:
                    self.connection.process_data_events(time_limit=1.0)
                except pika.exceptions.AMQPConnectionError:
                    print("Connection lost, stopping...")
                    break
                    
        except KeyboardInterrupt:
            print("\nReceived interrupt signal, stopping...")
        finally:
            self.running = False
            print(f"\nStopped consuming. Total responses received: {self.response_count}")

    def stop_consuming(self):
        """Stop consuming responses."""
        self.running = False


def signal_handler(signum, frame):
    """Handle interrupt signals gracefully."""
    print(f"\nReceived signal {signum}, stopping...")
    if hasattr(signal_handler, 'consumer'):
        signal_handler.consumer.stop_consuming()


def main():
    parser = argparse.ArgumentParser(description='Consumer for llama-messaging load testing')
    parser.add_argument('--host', default='localhost', help='RabbitMQ host')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ port')
    parser.add_argument('--username', default='guest', help='RabbitMQ username')
    parser.add_argument('--password', default='guest', help='RabbitMQ password')
    parser.add_argument('--count', type=int, help='Maximum number of responses to receive')
    parser.add_argument('--timeout', type=float, help='Timeout in seconds')
    
    args = parser.parse_args()
    
    # Create consumer
    consumer = LlamaMessagingConsumer(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password
    )
    
    # Set up signal handler
    signal_handler.consumer = consumer
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Connect to RabbitMQ
        if not consumer.connect():
            sys.exit(1)
        
        # Start consuming responses
        consumer.start_consuming(
            max_responses=args.count,
            timeout=args.timeout
        )
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        consumer.disconnect()


if __name__ == '__main__':
    main() 