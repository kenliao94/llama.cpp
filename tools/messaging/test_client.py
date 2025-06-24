#!/usr/bin/env python3
"""
Test client for llama-messaging tool.

This script demonstrates how to send inference requests to the llama-messaging tool
via RabbitMQ and receive responses.

Usage:
    python test_client.py [--host HOST] [--port PORT] [--prompt PROMPT]
"""

import argparse
import json
import pika
import sys
import time
import uuid
from typing import Dict, Any


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
        
        # Default queue and exchange names
        self.request_queue = 'llama_requests'
        self.response_queue = 'llama_responses'
        self.exchange = 'llama_exchange'
        self.routing_key = 'llama.inference'
        
        self.connection = None
        self.channel = None
        self.response_callback = None

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
            self.channel.queue_bind(exchange=self.exchange, queue=self.request_queue, routing_key=self.routing_key)
            
            print(f"Connected to RabbitMQ at {self.host}:{self.port}")
            return True
            
        except Exception as e:
            print(f"Failed to connect to RabbitMQ: {e}")
            return False

    def disconnect(self):
        """Close the RabbitMQ connection."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()

    def send_request(self, request: Dict[str, Any], timeout: float = 30.0) -> Dict[str, Any]:
        """Send an inference request and wait for response."""
        if not self.channel:
            raise RuntimeError("Not connected to RabbitMQ")
        
        # Generate request ID if not provided
        if 'id' not in request:
            request['id'] = str(uuid.uuid4())
        
        request_id = request['id']
        response_received = False
        response_data = None
        
        def response_callback(ch, method, properties, body):
            nonlocal response_received, response_data
            try:
                response = json.loads(body.decode('utf-8'))
                if response.get('id') == request_id:
                    response_data = response
                    response_received = True
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    ch.stop_consuming()
            except json.JSONDecodeError as e:
                print(f"Failed to parse response: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        # Set up response consumer
        self.channel.basic_consume(
            queue=self.response_queue,
            on_message_callback=response_callback,
            auto_ack=False
        )
        
        # Send request
        request_body = json.dumps(request)
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=request_body
        )
        
        print(f"Sent request {request_id}: {request.get('prompt', '')[:50]}...")
        
        # Wait for response
        start_time = time.time()
        while not response_received and (time.time() - start_time) < timeout:
            try:
                self.connection.process_data_events(time_limit=0.1)
            except pika.exceptions.AMQPConnectionError:
                print("Connection lost while waiting for response")
                break
        
        if not response_received:
            raise TimeoutError(f"Timeout waiting for response to request {request_id}")
        
        return response_data

    def send_simple_request(self, prompt: str, max_tokens: int = 128, 
                           temperature: float = 0.8, system_prompt: str = "") -> str:
        """Send a simple text generation request."""
        request = {
            'prompt': prompt,
            'max_tokens': max_tokens,
            'temperature': temperature
        }
        
        if system_prompt:
            request['system_prompt'] = system_prompt
        
        response = self.send_request(request)
        
        if 'error' in response:
            raise RuntimeError(f"Request failed: {response['error']}")
        
        return response.get('content', '')


def main():
    parser = argparse.ArgumentParser(description='Test client for llama-messaging')
    parser.add_argument('--host', default='localhost', help='RabbitMQ host')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ port')
    parser.add_argument('--username', default='guest', help='RabbitMQ username')
    parser.add_argument('--password', default='guest', help='RabbitMQ password')
    parser.add_argument('--prompt', default='Write a short poem about artificial intelligence.', 
                       help='Prompt to send')
    parser.add_argument('--max-tokens', type=int, default=128, help='Maximum tokens to generate')
    parser.add_argument('--temperature', type=float, default=0.8, help='Sampling temperature')
    parser.add_argument('--system-prompt', default='', help='System prompt')
    
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
        response = client.send_simple_request(
            prompt=args.prompt,
            max_tokens=args.max_tokens,
            temperature=args.temperature,
            system_prompt=args.system_prompt
        )
        
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