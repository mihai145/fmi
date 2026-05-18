#include "../../include/comm/RedisCheckpoint.h"
#include <boost/log/trivial.hpp>
#include <cmath>

FMI::Comm::RedisCheckpoint::RedisCheckpoint(std::map<std::string, std::string> params,
                                            std::map<std::string, std::string> model_params)
    : ClientServer(params) {
    hostname = params["host"];
    port = std::stoi(params["port"]);

    bandwidth_single = std::stod(model_params["bandwidth_single"]);
    bandwidth_multiple = std::stod(model_params["bandwidth_multiple"]);
    overhead = std::stod(model_params["overhead"]);
    transfer_price = std::stod(model_params["transfer_price"]);
    instance_price = std::stod(model_params["instance_price"]);
    requests_per_hour = std::stoi(model_params["requests_per_hour"]);
    if (model_params["include_infrastructure_costs"] == "true") {
        include_infrastructure_costs = true;
    } else {
        include_infrastructure_costs = false;
    }

    restore_fn();

    checkpointer = std::make_unique<checkpoint::Checkpointer>(
        [this] {
            checkpointer->register_hint(state);
            teardown_fn();
        },
        [this] { restore_fn(); });
}

FMI::Comm::RedisCheckpoint::~RedisCheckpoint() {
    checkpointer->shutdown_ctrl_thread();
    checkpointer->register_hint(state);
    teardown_fn();
}

void FMI::Comm::RedisCheckpoint::upload_object(channel_data buf, std::string name) {
    auto ctx = checkpointer->get_uninterruptible_context();
    std::string command = "SET " + name + " %b";
    auto *reply = (redisReply *)redisCommand(context, command.c_str(), buf.buf, buf.len);
    bool ok = reply->type != REDIS_REPLY_ERROR;
    if (!ok)
        BOOST_LOG_TRIVIAL(error) << "Error when uploading to Redis: " << reply->str;
    freeReplyObject(reply);
}

bool FMI::Comm::RedisCheckpoint::download_object(channel_data buf, std::string name) {
    auto ctx = checkpointer->get_uninterruptible_context();
    std::string command = "GET " + name;
    auto *reply = (redisReply *)redisCommand(context, command.c_str());
    bool ok = reply->type != REDIS_REPLY_NIL && reply->type != REDIS_REPLY_ERROR;
    if (ok)
        std::memcpy(buf.buf, reply->str, std::min(buf.len, reply->len));
    freeReplyObject(reply);

    try {
        int peer = std::stoi(name.substr(comm_name.size()));
        if (peer >= 0 && peer < state.num_peers)
            state.waiting_for[peer] = !ok;
    } catch (...) {
    }

    return ok;
}

void FMI::Comm::RedisCheckpoint::delete_object(std::string name) {
    auto ctx = checkpointer->get_uninterruptible_context();
    std::string command = "DEL " + name;
    auto *reply = (redisReply *)redisCommand(context, command.c_str());
    freeReplyObject(reply);
}

std::vector<std::string> FMI::Comm::RedisCheckpoint::get_object_names() {
    auto ctx = checkpointer->get_uninterruptible_context();
    std::vector<std::string> keys;
    std::string command = "KEYS *";
    auto *reply = (redisReply *)redisCommand(context, command.c_str());
    for (int i = 0; i < reply->elements; i++) {
        keys.emplace_back(reply->element[i]->str);
    }
    freeReplyObject(reply);

    if (std::holds_alternative<checkpoint::Barrier>(state.current_state)) {
        unsigned int barrier_num = num_operations.at("barrier") - 1;
        std::string barrier_suffix = "_barrier_" + std::to_string(barrier_num);
        for (int i = 0; i < state.num_peers; i++) {
            std::string expected = comm_name + std::to_string(i) + barrier_suffix;
            state.waiting_for[i] = std::find(keys.begin(), keys.end(), expected) == keys.end();
        }
    }

    return keys;
}

double FMI::Comm::RedisCheckpoint::get_latency(Utils::peer_num producer, Utils::peer_num consumer,
                                               std::size_t size_in_bytes) {
    double agg_bandwidth = std::min(producer * consumer * bandwidth_single, bandwidth_multiple);
    double trans_time = producer * consumer * ((double)size_in_bytes / 1000000.) / agg_bandwidth;
    return log2(producer + consumer) * overhead + trans_time;
}

double FMI::Comm::RedisCheckpoint::get_price(Utils::peer_num producer, Utils::peer_num consumer,
                                             std::size_t size_in_bytes) {
    double transfer_costs = (1 + consumer) * producer * ((double)size_in_bytes / 1000000000.) * transfer_price;
    double total_costs = transfer_costs;
    if (include_infrastructure_costs) {
        total_costs += 1. / requests_per_hour * instance_price;
    }
    return total_costs;
}

void FMI::Comm::RedisCheckpoint::teardown_fn() {
    redisFree(context);
    context = nullptr;
}

void FMI::Comm::RedisCheckpoint::restore_fn() {
    context = redisConnect(hostname.c_str(), port);
    if (context == nullptr || context->err) {
        if (context) {
            BOOST_LOG_TRIVIAL(error) << "Error when connecting to Redis: " << context->errstr;
        } else {
            BOOST_LOG_TRIVIAL(error) << "Allocating Redis context not possible";
        }
    }
}
