#!/usr/bin/env python3
"""
Producer for llama-messaging tool.

This script sends inference requests to the llama-messaging tool via RabbitMQ
without waiting for responses. Useful for load testing.

Usage:
    python producer.py [--host HOST] [--port PORT] [--prompt PROMPT] [--count COUNT]
"""

import argparse
import pika
import sys
import time
import uuid
from typing import Optional


class LlamaMessagingProducer:
    def __init__(self, host: str = 'localhost', port: int = 5672, 
                 username: str = 'guest', password: str = 'guest',
                 vhost: str = '/'):
        """Initialize the producer with RabbitMQ connection parameters."""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        
        # Queue and exchange names to match messaging.cpp
        self.request_queue = 'inference_request'
        self.exchange = 'amq.direct'
        self.request_routing_key = 'inference.request'
        
        self.connection = None
        self.channel = None

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
            self.channel.queue_declare(queue=self.request_queue, durable=True)
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=True)
            
            # Bind queue to exchange with routing key
            self.channel.queue_bind(
                exchange=self.exchange, 
                queue=self.request_queue, 
                routing_key=self.request_routing_key
            )
            
            print(f"Connected to RabbitMQ at {self.host}:{self.port}")
            print(f"Request queue: {self.request_queue}")
            return True
            
        except Exception as e:
            print(f"Failed to connect to RabbitMQ: {e}")
            return False

    def disconnect(self):
        """Close the RabbitMQ connection."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def send_request(self, prompt: str, request_id: str = None) -> str:
        """Send an inference request without waiting for response."""
        if not self.channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        if request_id is None:
            request_id = str(uuid.uuid4())
        
        # Send request (plain text, not JSON)
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.request_routing_key,
            body=prompt,
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent message
                content_type='text/plain',
                message_id=request_id
            )
        )
        
        return request_id

    def send_multiple_requests(self, prompt: str, count: int, delay: float = 0.0):
        """Send multiple requests with optional delay between them."""
        print(f"Sending {count} requests with prompt: {prompt[:50]}...")
        
        for i in range(count):
            request_id = self.send_request(prompt)
            print(f"Sent request {i+1}/{count} with ID: {request_id}")
            
            if delay > 0 and i < count - 1:  # Don't delay after the last request
                time.sleep(delay)
        
        print(f"Successfully sent {count} requests")


def main():
    parser = argparse.ArgumentParser(description='Producer for llama-messaging load testing')
    parser.add_argument('--host', default='localhost', help='RabbitMQ host')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ port')
    parser.add_argument('--username', default='guest', help='RabbitMQ username')
    parser.add_argument('--password', default='guest', help='RabbitMQ password')
    parser.add_argument('--prompt', default='Write a short poem about artificial intelligence.', 
                       help='Prompt to send')
    parser.add_argument('--count', type=int, default=1, help='Number of requests to send')
    parser.add_argument('--delay', type=float, default=0.0, help='Delay between requests in seconds')
    
    args = parser.parse_args()
    
    # Create producer
    producer = LlamaMessagingProducer(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password
    )
    
    try:
        # Connect to RabbitMQ
        if not producer.connect():
            sys.exit(1)
        
        # Send requests
        producer.send_multiple_requests(
            prompt=args.prompt,
            count=args.count,
            delay=args.delay
        )
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        producer.disconnect()


if __name__ == '__main__':
    main() 