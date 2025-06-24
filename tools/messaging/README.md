# llama-messaging

A RabbitMQ-based inference tool for llama.cpp that allows you to run inference requests through message queues.

## Overview

llama-messaging connects to a RabbitMQ instance and listens for inference requests on a specified queue. It processes these requests using a loaded llama.cpp model and sends responses back through the message queue system.

## Features

- **Message Queue Integration**: Uses RabbitMQ for reliable message processing
- **JSON-based API**: Simple JSON request/response format
- **Chat Template Support**: Supports chat templates for conversation-style interactions
- **Configurable Queues**: Customizable queue names and routing keys
- **Error Handling**: Comprehensive error handling and reporting
- **Graceful Shutdown**: Proper signal handling for clean shutdowns

## Prerequisites

### System Dependencies

1. **RabbitMQ Server**: Install and run RabbitMQ server
2. **AMQP C++ Client**: Install the AMQP C++ client library

#### Installing RabbitMQ

**Ubuntu/Debian:**
```bash
sudo apt-get install rabbitmq-server
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
```

**macOS:**
```bash
brew install rabbitmq
brew services start rabbitmq
```

**Windows:**
Download and install from [RabbitMQ website](https://www.rabbitmq.com/download.html)

#### Installing AMQP C++ Client

**Ubuntu/Debian:**
```bash
sudo apt-get install libamqpcpp-dev
```

**macOS:**
```bash
brew install amqpcpp
```

**From Source:**
```bash
git clone https://github.com/CopernicaMarketingSoftware/AMQP-CPP.git
cd AMQP-CPP
mkdir build && cd build
cmake ..
make
sudo make install
```

## Building

Add the messaging tool to your build by including it in `tools/CMakeLists.txt`:

```cmake
add_subdirectory(messaging)
```

Then build as usual:
```bash
cmake --build build --config Release
```

## Usage

### Basic Usage

```bash
./llama-messaging -m /path/to/model.gguf --amqp-url amqp://localhost:5672
```

### Advanced Usage

```bash
./llama-messaging \
  -m /path/to/model.gguf \
  --amqp-url amqp://user:password@rabbitmq.example.com:5672/vhost \
  --request-queue my_requests \
  --response-queue my_responses \
  --exchange my_exchange \
  --routing-key inference.request \
  --prefetch-count 5 \
  --n-ctx 4096 \
  --temp 0.8
```

### Command Line Options

#### RabbitMQ Options

- `--amqp-url URL`: RabbitMQ connection URL (default: `amqp://localhost:5672`)
- `--request-queue QUEUE`: Request queue name (default: `llama_requests`)
- `--response-queue QUEUE`: Response queue name (default: `llama_responses`)
- `--exchange EXCHANGE`: Exchange name (default: `llama_exchange`)
- `--routing-key KEY`: Routing key (default: `llama.inference`)
- `--prefetch-count N`: Prefetch count (default: `1`)
- `--no-durable`: Make queues non-durable (default: durable)

#### Standard llama.cpp Options

All standard llama.cpp options are supported:
- `-m, --model`: Model file path
- `--n-ctx`: Context size
- `--temp`: Temperature for sampling
- `--top-p`: Top-p sampling
- `--top-k`: Top-k sampling
- `--repeat-penalty`: Repeat penalty
- `--chat-template`: Chat template
- And many more...

## API Reference

### Request Format

Send JSON messages to the request queue:

```json
{
  "id": "request-123",
  "prompt": "What is the capital of France?",
  "system_prompt": "You are a helpful assistant.",
  "max_tokens": 128,
  "temperature": 0.8,
  "top_p": 0.95,
  "top_k": 40,
  "stream": false,
  "stop_sequences": ["\n", "END"],
  "model_name": "llama-2-7b",
  "chat_format": "chatml"
}
```

#### Request Fields

- `id` (string, required): Unique request identifier
- `prompt` (string, required): The input prompt
- `system_prompt` (string, optional): System prompt for chat models
- `max_tokens` (integer, optional): Maximum tokens to generate (default: 128)
- `temperature` (float, optional): Sampling temperature (default: 0.8)
- `top_p` (float, optional): Top-p sampling (default: 0.95)
- `top_k` (integer, optional): Top-k sampling (default: 40)
- `stream` (boolean, optional): Enable streaming (not yet implemented)
- `stop_sequences` (array, optional): Stop generation on these sequences
- `model_name` (string, optional): Model name for logging
- `chat_format` (string, optional): Chat format to use

### Response Format

Responses are published to the response queue:

```json
{
  "id": "request-123",
  "content": "The capital of France is Paris.",
  "finished": true,
  "finish_reason": "length",
  "prompt_tokens": 15,
  "completion_tokens": 8,
  "total_time_ms": 1250.5
}
```

#### Response Fields

- `id` (string): Request identifier (echoed from request)
- `content` (string): Generated text content
- `finished` (boolean): Whether generation is complete
- `finish_reason` (string): Reason for stopping ("length", "stop", "eos")
- `prompt_tokens` (integer): Number of tokens in prompt
- `completion_tokens` (integer): Number of generated tokens
- `total_time_ms` (float): Total processing time in milliseconds
- `error` (string, optional): Error message if processing failed

## Examples

### Python Client Example

```python
import pika
import json
import uuid

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare queues
channel.queue_declare(queue='llama_requests', durable=True)
channel.queue_declare(queue='llama_responses', durable=True)

# Send request
request = {
    "id": str(uuid.uuid4()),
    "prompt": "Explain quantum computing in simple terms.",
    "max_tokens": 200,
    "temperature": 0.7
}

channel.basic_publish(
    exchange='llama_exchange',
    routing_key='llama.inference',
    body=json.dumps(request)
)

print(f"Sent request: {request['id']}")

# Receive response
def callback(ch, method, properties, body):
    response = json.loads(body)
    if response['id'] == request['id']:
        print(f"Response: {response['content']}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        connection.close()

channel.basic_consume(queue='llama_responses', on_message_callback=callback)
channel.start_consuming()
```

### Node.js Client Example

```javascript
const amqp = require('amqplib');

async function sendRequest() {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    
    const request = {
        id: Date.now().toString(),
        prompt: "Write a haiku about programming",
        max_tokens: 50,
        temperature: 0.8
    };
    
    await channel.assertQueue('llama_requests', { durable: true });
    await channel.assertQueue('llama_responses', { durable: true });
    
    channel.sendToQueue('llama_requests', Buffer.from(JSON.stringify(request)));
    console.log(`Sent request: ${request.id}`);
    
    // Listen for response
    channel.consume('llama_responses', (msg) => {
        const response = JSON.parse(msg.content.toString());
        if (response.id === request.id) {
            console.log(`Response: ${response.content}`);
            channel.ack(msg);
            connection.close();
        }
    });
}

sendRequest().catch(console.error);
```

## Configuration

### RabbitMQ Configuration

The tool uses standard RabbitMQ configuration. You can customize:

1. **Virtual Hosts**: Use different vhosts for different environments
2. **Users and Permissions**: Set up specific users with appropriate permissions
3. **Queue Durability**: Use durable queues for persistence across restarts
4. **Exchange Types**: Currently supports direct exchanges

### Performance Tuning

- **Prefetch Count**: Adjust based on your processing capacity
- **Queue Durability**: Use durable queues for reliability, non-durable for performance
- **Context Size**: Balance memory usage with generation quality
- **Batch Processing**: Consider using multiple instances for high throughput

## Troubleshooting

### Common Issues

1. **Connection Failed**: Check RabbitMQ server is running and accessible
2. **Queue Not Found**: Ensure queues are declared before use
3. **Permission Denied**: Verify user has appropriate permissions
4. **Model Loading Failed**: Check model file path and format

### Logging

The tool uses the standard llama.cpp logging system. Enable verbose logging:

```bash
./llama-messaging -m model.gguf --verbose
```

### Monitoring

Monitor RabbitMQ queues using the management interface:

```bash
# Enable management plugin
sudo rabbitmq-plugins enable rabbitmq_management

# Access at http://localhost:15672
# Default credentials: guest/guest
```

## Security Considerations

1. **Network Security**: Use TLS/SSL for production deployments
2. **Authentication**: Use strong credentials for RabbitMQ users
3. **Authorization**: Limit user permissions to necessary queues/exchanges
4. **Input Validation**: Validate all incoming requests
5. **Resource Limits**: Set appropriate limits to prevent abuse

## Contributing

Contributions are welcome! Please:

1. Follow the existing code style
2. Add appropriate tests
3. Update documentation
4. Ensure compatibility with existing llama.cpp features

## License

This tool is part of llama.cpp and follows the same license terms. 