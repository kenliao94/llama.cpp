#include "arg.h"
#include "common.h"
#include "log.h"
#include "sampling.h"
#include "llama.h"

#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

static void print_usage(int argc, char ** argv) {
    (void) argc;

    LOG("\nexample usage:\n");
    LOG("\n  %s -m your_model.gguf\n", argv[0]);
    LOG("\n");
}

int main(int argc, char ** argv) {
    common_params params;
    
    if (!common_params_parse(argc, argv, params, LLAMA_EXAMPLE_MAIN, print_usage)) {
        return 1;
    }

    common_init();

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
    
    // cleanup
    common_sampler_free(smpl);
    llama_backend_free();
    
    return 0;
} 