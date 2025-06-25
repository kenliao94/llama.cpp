#!/usr/bin/env python3
"""
Test client for llama-messaging tool.

This script demonstrates how to send inference requests to the llama-messaging tool
via RabbitMQ and receive responses.

Usage:
    python test_client.py [--host HOST] [--port PORT] [--prompt PROMPT]
"""

import argparse
import pika
import sys
import time
from typing import Optional


class LlamaMessagingClient:
    def __init__(self, host: str = 'localhost', port: int = 5672, 
                 username: str = 'guest', password: str = 'guest',
                 vhost: str = '/'):
        """Initialize the client with RabbitMQ connection parameters."""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        
        # Updated queue and exchange names to match messaging.cpp
        self.request_queue = 'inference_request'
        self.response_queue = 'inference_response'
        self.exchange = 'amq.direct'
        self.request_routing_key = 'inference.request'
        self.response_routing_key = 'inference.response'
        
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
            
            # Declare queues and exchange
            self.channel.queue_declare(queue=self.request_queue, durable=True)
            self.channel.queue_declare(queue=self.response_queue, durable=True)
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=True)
            
            # Bind queues to exchange with routing keys
            self.channel.queue_bind(
                exchange=self.exchange, 
                queue=self.request_queue, 
                routing_key=self.request_routing_key
            )
            self.channel.queue_bind(
                exchange=self.exchange, 
                queue=self.response_queue, 
                routing_key=self.response_routing_key
            )
            
            print(f"Connected to RabbitMQ at {self.host}:{self.port}")
            print(f"Request queue: {self.request_queue}")
            print(f"Response queue: {self.response_queue}")
            return True
            
        except Exception as e:
            print(f"Failed to connect to RabbitMQ: {e}")
            return False

    def disconnect(self):
        """Close the RabbitMQ connection."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def send_request(self, prompt: str, timeout: float = 30.0) -> str:
        """Send an inference request and wait for response."""
        if not self.channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        response_received = False
        response_data = None
        
        def response_callback(ch, method, properties, body):
            nonlocal response_received, response_data
            try:
                # Response is now plain text, not JSON
                response_data = body.decode('utf-8')
                response_received = True
                ch.basic_ack(delivery_tag=method.delivery_tag)
                ch.stop_consuming()
            except Exception as e:
                print(f"Failed to parse response: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        # Set up response consumer
        self.channel.basic_consume(
            queue=self.response_queue,
            on_message_callback=response_callback,
            auto_ack=False
        )
        
        # Send request (plain text, not JSON)
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.request_routing_key,
            body=prompt,
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent message
                content_type='text/plain'
            )
        )
        
        print(f"Sent request: {prompt[:50]}...")
        
        # Wait for response
        start_time = time.time()
        while not response_received and (time.time() - start_time) < timeout:
            try:
                self.connection.process_data_events(time_limit=0.1)
            except pika.exceptions.AMQPConnectionError:
                print("Connection lost while waiting for response")
                break
        
        if not response_received:
            raise TimeoutError(f"Timeout waiting for response")
        
        return response_data

    def send_simple_request(self, prompt: str) -> str:
        """Send a simple text generation request."""
        return self.send_request(prompt)


def main():
    parser = argparse.ArgumentParser(description='Test client for llama-messaging')
    parser.add_argument('--host', default='localhost', help='RabbitMQ host')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ port')
    parser.add_argument('--username', default='guest', help='RabbitMQ username')
    parser.add_argument('--password', default='guest', help='RabbitMQ password')
    parser.add_argument('--prompt', default='Write a short poem about artificial intelligence.', 
                       help='Prompt to send')
    
    args = parser.parse_args()
    
    # Create client
    client = LlamaMessagingClient(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password
    )
    
    try:
        # Connect to RabbitMQ
        if not client.connect():
            sys.exit(1)
        
        # Send request
        print(f"Sending prompt: {args.prompt}")
        response = client.send_simple_request(prompt=args.prompt)
        
        print("\n" + "="*50)
        print("RESPONSE:")
        print("="*50)
        print(response)
        print("="*50)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        client.disconnect()


if __name__ == '__main__':
    main() 