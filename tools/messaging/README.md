# Llama.cpp Messaging Tool with RabbitMQ Integration

This tool extends the basic llama.cpp functionality with RabbitMQ messaging capabilities, allowing you to send and receive messages through a RabbitMQ broker.

## Features

- **LLaMA Model Integration**: Uses llama.cpp for text generation
- **RabbitMQ Messaging**: Optional RabbitMQ integration for message queuing
- **Configurable**: Supports various RabbitMQ connection parameters
- **Graceful Fallback**: Works without RabbitMQ if not available

## Prerequisites

### Required
- CMake 3.14 or higher
- C++17 compatible compiler
- llama.cpp dependencies

### Optional (for RabbitMQ support)
- RabbitMQ C client library (`librabbitmq-dev`)
- RabbitMQ server running locally or remotely

## Building

### With RabbitMQ Support
```bash
# Install RabbitMQ C client library
# On Ubuntu/Debian:
sudo apt-get install librabbitmq-dev

# On macOS:
brew install rabbitmq-c

# Build the project
mkdir build && cd build
cmake ..
make llama-messaging
```

### Without RabbitMQ Support
The tool will build and work without RabbitMQ if the library is not found:
```bash
mkdir build && cd build
cmake ..
make llama-messaging
```

## Usage

### Basic Usage (without RabbitMQ)
```bash
./llama-messaging -m path/to/model.gguf -p "Hello, how are you?"
```

### With RabbitMQ Integration
```bash
# Basic usage with RabbitMQ (uses default settings)
./llama-messaging -m path/to/model.gguf -p "Hello, how are you?"

# With custom RabbitMQ settings
./llama-messaging -m path/to/model.gguf \
  --rmq-host rabbitmq.example.com \
  --rmq-port 5672 \
  --rmq-username myuser \
  --rmq-password mypass \
  --rmq-exchange my_exchange \
  --rmq-routing-key my_routing_key \
  --rmq-queue my_queue \
  -p "Hello, how are you?"
```

## RabbitMQ Configuration

The tool supports the following RabbitMQ parameters:

- `--rmq-host`: RabbitMQ server host (default: localhost)
- `--rmq-port`: RabbitMQ server port (default: 5672)
- `--rmq-username`: RabbitMQ username (default: guest)
- `--rmq-password`: RabbitMQ password (default: guest)
- `--rmq-vhost`: RabbitMQ virtual host (default: /)
- `--rmq-exchange`: RabbitMQ exchange name (default: amq.direct)
- `--rmq-routing-key`: RabbitMQ routing key (default: llama.messages)
- `--rmq-queue`: RabbitMQ queue name (default: llama_queue)

## Message Format

When RabbitMQ is enabled, the tool sends messages in the following format:
```
Prompt: [user prompt]
Response: [generated response]
```

## Example Workflow

1. **Start RabbitMQ Server**:
   ```bash
   # Using Docker
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management
   ```

2. **Run the Messaging Tool**:
   ```bash
   ./llama-messaging -m model.gguf -p "What is the capital of France?"
   ```

3. **Monitor Messages**:
   - Access RabbitMQ Management UI at http://localhost:15672
   - Use default credentials: guest/guest
   - Check the `llama_queue` for incoming messages

## Troubleshooting

### RabbitMQ Connection Issues
- Ensure RabbitMQ server is running
- Check firewall settings
- Verify connection parameters (host, port, credentials)
- Check RabbitMQ logs for authentication errors

### Build Issues
- If RabbitMQ headers are not found, the tool will build without RabbitMQ support
- Install `librabbitmq-dev` package for RabbitMQ integration
- Ensure pkg-config is available on your system

### Runtime Issues
- The tool will continue to work even if RabbitMQ connection fails
- Check logs for connection error messages
- Verify RabbitMQ server is accessible from the build machine

## License

This tool is part of llama.cpp and follows the same license terms. 