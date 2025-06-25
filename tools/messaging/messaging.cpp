#include "arg.h"
#include "common.h"
#include "log.h"
#include "sampling.h"
#include "llama.h"

#ifdef LLAMA_USE_RABBITMQ
// RabbitMQ C client headers
#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>
#include <amqp_ssl_socket.h>
#endif

#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#ifdef LLAMA_USE_RABBITMQ
// RabbitMQ connection parameters
struct rabbitmq_params {
    std::string host = "localhost";
    int port = 5672;
    std::string username = "guest";
    std::string password = "guest";
    std::string vhost = "/";
    std::string exchange = "amq.direct";
    std::string routing_key = "llama.messages";
    std::string queue = "llama_queue";
};

static rabbitmq_params rmq_params;
#endif

static void print_usage(int argc, char ** argv) {
    (void) argc;

    LOG("\nexample usage:\n");
    LOG("\n  %s -m your_model.gguf\n", argv[0]);
#ifdef LLAMA_USE_RABBITMQ
    LOG("\nRabbitMQ options:\n");
    LOG("  --rmq-host HOST        RabbitMQ host (default: localhost)\n");
    LOG("  --rmq-port PORT        RabbitMQ port (default: 5672)\n");
    LOG("  --rmq-username USER    RabbitMQ username (default: guest)\n");
    LOG("  --rmq-password PASS    RabbitMQ password (default: guest)\n");
    LOG("  --rmq-vhost VHOST      RabbitMQ vhost (default: /)\n");
    LOG("  --rmq-exchange EXCH    RabbitMQ exchange (default: amq.direct)\n");
    LOG("  --rmq-routing-key KEY  RabbitMQ routing key (default: llama.messages)\n");
    LOG("  --rmq-queue QUEUE      RabbitMQ queue name (default: llama_queue)\n");
#endif
    LOG("\n");
}

#ifdef LLAMA_USE_RABBITMQ
// Function to handle RabbitMQ connection
amqp_connection_state_t connect_rabbitmq() {
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    
    if (!socket) {
        LOG_ERR("Failed to create TCP socket\n");
        return nullptr;
    }
    
    int status = amqp_socket_open(socket, rmq_params.host.c_str(), rmq_params.port);
    if (status) {
        LOG_ERR("Failed to open socket to %s:%d\n", rmq_params.host.c_str(), rmq_params.port);
        return nullptr;
    }
    
    amqp_rpc_reply_t reply = amqp_login(conn, rmq_params.vhost.c_str(), 0, 131072, 0, 
                                       AMQP_SASL_METHOD_PLAIN, rmq_params.username.c_str(), 
                                       rmq_params.password.c_str());
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG_ERR("Failed to login to RabbitMQ\n");
        return nullptr;
    }
    
    amqp_channel_open(conn, 1);
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG_ERR("Failed to open channel\n");
        return nullptr;
    }
    
    // Declare queue
    amqp_queue_declare(conn, 1, amqp_cstring_bytes(rmq_params.queue.c_str()), 0, 1, 0, 0, 
                      amqp_empty_table);
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG_ERR("Failed to declare queue\n");
        return nullptr;
    }
    
    // Bind queue to exchange
    amqp_queue_bind(conn, 1, amqp_cstring_bytes(rmq_params.queue.c_str()), 
                   amqp_cstring_bytes(rmq_params.exchange.c_str()), 
                   amqp_cstring_bytes(rmq_params.routing_key.c_str()), 
                   amqp_empty_table);
    reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        LOG_ERR("Failed to bind queue\n");
        return nullptr;
    }
    
    LOG_INF("Connected to RabbitMQ at %s:%d\n", rmq_params.host.c_str(), rmq_params.port);
    return conn;
}

// Function to send message to RabbitMQ
bool send_rabbitmq_message(amqp_connection_state_t conn, const std::string& message) {
    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2; // persistent message
    
    int result = amqp_basic_publish(conn, 1, amqp_cstring_bytes(rmq_params.exchange.c_str()),
                                   amqp_cstring_bytes(rmq_params.routing_key.c_str()), 0, 0,
                                   &props, amqp_cstring_bytes(message.c_str()));
    
    if (result < 0) {
        LOG_ERR("Failed to publish message to RabbitMQ\n");
        return false;
    }
    
    LOG_INF("Sent message to RabbitMQ: %s\n", message.c_str());
    return true;
}

// Function to receive message from RabbitMQ
std::string receive_rabbitmq_message(amqp_connection_state_t conn) {
    amqp_rpc_reply_t reply;
    amqp_envelope_t envelope;
    
    reply = amqp_consume_message(conn, &envelope, nullptr, 0);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        return "";
    }
    
    std::string message((char*)envelope.message.body.bytes, envelope.message.body.len);
    amqp_destroy_envelope(&envelope);
    
    LOG_INF("Received message from RabbitMQ: %s\n", message.c_str());
    return message;
}
#endif

int main(int argc, char ** argv) {
    common_params params;
    
    if (!common_params_parse(argc, argv, params, LLAMA_EXAMPLE_MAIN, print_usage)) {
        return 1;
    }

    common_init();

#ifdef LLAMA_USE_RABBITMQ
    // Initialize RabbitMQ connection
    amqp_connection_state_t rmq_conn = nullptr;
    rmq_conn = connect_rabbitmq();
    if (!rmq_conn) {
        LOG_ERR("Failed to connect to RabbitMQ. Continuing without RabbitMQ support.\n");
    }
#endif

    // Set default prompt if not provided
    if (params.prompt.empty()) {
        params.prompt = "hello, what is your name";
    }

    // Set default number of tokens to predict if not provided
    if (params.n_predict == -1) {
        params.n_predict = 128;
    }

    LOG_INF("%s: llama backend init\n", __func__);

    llama_backend_init();
    llama_numa_init(params.numa);

    // load the model
    LOG_INF("%s: load the model\n", __func__);
    common_init_result llama_init = common_init_from_params(params);

    llama_model * model = llama_init.model.get();
    llama_context * ctx = llama_init.context.get();

    if (model == NULL) {
        LOG_ERR("%s: error: unable to load model\n", __func__);
        return 1;
    }

    const llama_vocab * vocab = llama_model_get_vocab(model);
    const int n_ctx = llama_n_ctx(ctx);

    LOG_INF("context size = %d\n", n_ctx);

    // tokenize the prompt
    LOG_INF("tokenizing prompt: '%s'\n", params.prompt.c_str());
    std::vector<llama_token> embd_inp = common_tokenize(ctx, params.prompt, true, true);

    if (embd_inp.empty()) {
        LOG_ERR("%s: error: empty prompt\n", __func__);
        return 1;
    }

    // check if prompt is too long
    if ((int) embd_inp.size() > n_ctx - 4) {
        LOG_ERR("%s: prompt is too long (%d tokens, max %d)\n", __func__, (int) embd_inp.size(), n_ctx - 4);
        return 1;
    }

    // initialize sampler
    auto & sparams = params.sampling;
    common_sampler * smpl = common_sampler_init(model, sparams);
    if (!smpl) {
        LOG_ERR("%s: failed to initialize sampling subsystem\n", __func__);
        return 1;
    }

    LOG_INF("sampler seed: %u\n", common_sampler_get_seed(smpl));
    LOG_INF("sampler params: \n%s\n", sparams.print().c_str());

    // evaluate the prompt
    LOG_INF("evaluating prompt...\n");
    
    for (int i = 0; i < (int) embd_inp.size(); i += params.n_batch) {
        int n_eval = (int) embd_inp.size() - i;
        if (n_eval > params.n_batch) {
            n_eval = params.n_batch;
        }

        if (llama_decode(ctx, llama_batch_get_one(&embd_inp[i], n_eval))) {
            LOG_ERR("%s : failed to eval\n", __func__);
            return 1;
        }
    }

    // accept all prompt tokens into the sampler
    for (auto id : embd_inp) {
        common_sampler_accept(smpl, id, false);
    }

    // generate tokens
    LOG_INF("generating response...\n");
    
    int n_past = embd_inp.size();
    int n_remain = params.n_predict;
    
    std::string response;
    
    while (n_remain > 0) {
        // sample the next token
        const llama_token id = common_sampler_sample(smpl, ctx, -1);
        
        // accept the token
        common_sampler_accept(smpl, id, true);
        
        // convert token to text
        const std::string token_str = common_token_to_piece(ctx, id, params.special);
        response += token_str;
        
        // print the token
        LOG("%s", token_str.c_str());
        
        // check for end of generation
        if (llama_vocab_is_eog(vocab, id)) {
            LOG("\n[end of text]\n");
            break;
        }
        
        // decode the token
        llama_token token_to_decode = id; // create non-const copy
        if (llama_decode(ctx, llama_batch_get_one(&token_to_decode, 1))) {
            LOG_ERR("%s : failed to eval\n", __func__);
            return 1;
        }
        
        n_past += 1;
        n_remain -= 1;
        
        // check if we've reached the context limit
        if (n_past >= n_ctx) {
            LOG("\n[context limit reached]\n");
            break;
        }
    }
    
    LOG("\n\n");
    
#ifdef LLAMA_USE_RABBITMQ
    // Send the complete response to RabbitMQ if connected
    if (rmq_conn) {
        std::string message = "Prompt: " + params.prompt + "\nResponse: " + response;
        if (send_rabbitmq_message(rmq_conn, message)) {
            LOG_INF("Response sent to RabbitMQ successfully\n");
        } else {
            LOG_ERR("Failed to send response to RabbitMQ\n");
        }
        
        // Close RabbitMQ connection
        amqp_channel_close(rmq_conn, 1, AMQP_REPLY_SUCCESS);
        amqp_connection_close(rmq_conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(rmq_conn);
    }
#endif
    
    // cleanup
    common_sampler_free(smpl);
    llama_backend_free();
    
    return 0;
} 