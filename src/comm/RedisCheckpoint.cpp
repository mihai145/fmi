#include "../../include/comm/RedisCheckpoint.h"

#include "utils.hpp"

#include <boost/log/trivial.hpp>
#include <chrono>
#include <cmath>
#include <cstdlib>

namespace {
    constexpr int ETCD_POLL_MS = 200;
    constexpr int GET_ENTRIES_RETRIES = 10;
    constexpr int SELF_STOP_TIMEOUT_MS = 3000;
} // namespace

FMI::Comm::RedisCheckpoint::RedisCheckpoint(std::map<std::string, std::string> params,
                                            std::map<std::string, std::string> model_params)
    : ClientServer(params) {
    hostname = params["host"];
    port = std::stoi(params["port"]);
    etcd_host = params["etcd_host"];
    etcd_port = std::stoi(params["etcd_port"]);
    func_id = std::stoi(getenv("func_id"));

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

    state.channel_type = 1; // object storage

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
    double start_ms = common::now_monotonic_ms();
    auto *reply = (redisReply *)redisCommand(context, command.c_str(), buf.buf, buf.len);
    account_query(start_ms);
    bool ok = reply->type != REDIS_REPLY_ERROR;
    if (!ok)
        BOOST_LOG_TRIVIAL(error) << "Error when uploading to Redis: " << reply->str;
    freeReplyObject(reply);
}

bool FMI::Comm::RedisCheckpoint::download_object(channel_data buf, std::string name) {
    auto ctx = checkpointer->get_uninterruptible_context();
    std::string command = "GET " + name;
    double start_ms = common::now_monotonic_ms();
    auto *reply = (redisReply *)redisCommand(context, command.c_str());
    account_query(start_ms);
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
    double start_ms = common::now_monotonic_ms();
    auto *reply = (redisReply *)redisCommand(context, command.c_str());
    account_query(start_ms);
    freeReplyObject(reply);
}

std::vector<std::string> FMI::Comm::RedisCheckpoint::get_object_names() {
    auto ctx = checkpointer->get_uninterruptible_context();
    std::vector<std::string> keys;
    std::string command = "KEYS *";
    double start_ms = common::now_monotonic_ms();
    auto *reply = (redisReply *)redisCommand(context, command.c_str());
    account_query(start_ms);
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
    shutdown.store(true);

    if (coordinator) {
        try {
            coordinator->delete_own_key(func_id);
        } catch (const std::exception &e) {
            BOOST_LOG_TRIVIAL(warning) << "teardown_fn(): delete_own_key failed: " << e.what();
        }
    }

    if (etcd_thread.joinable())
        etcd_thread.join();
    if (coordinator)
        coordinator->stop_watch();

    redisFree(context);
    context = nullptr;

    publish_wait();
}

void FMI::Comm::RedisCheckpoint::wait_tick(bool waiting) {
    auto ctx = checkpointer->get_uninterruptible_context();

    auto now = std::chrono::steady_clock::now();
    int cnt_restore = checkpointer->get_cnt_restore();

    // an interval straddling a checkpoint is suspension, not blocked time
    if (wait.armed && wait.cnt_restore == cnt_restore)
        wait_ms += std::chrono::duration<double, std::milli>(now - wait.since).count();

    wait = {waiting, cnt_restore, now};
}

void FMI::Comm::RedisCheckpoint::account_query(double start_ms) {
    redis_ms += common::now_monotonic_ms() - start_ms;
    redis_queries++;
}

void FMI::Comm::RedisCheckpoint::publish_wait() {
    auto metrics = common::func_metrics(getenv("job_id"), std::to_string(func_id),
                                        checkpointer->get_cnt_restore());
    metrics.log("comm_blocked", {{"blocked_ms", std::to_string(wait_ms)}});
    metrics.log("redis_time", {{"redis_ms", std::to_string(redis_ms)},
                               {"queries", std::to_string(redis_queries)}});
    wait_ms = 0;
    redis_ms = 0;
    redis_queries = 0;
}

void FMI::Comm::RedisCheckpoint::waiting_on(const std::vector<int> &peers) {
    wait_tick(!peers.empty());

    if (peers.empty()) {    // wait is over
        stall.armed = false;
        return;
    }

    // if at least one peer is live, we can make progress
    for (int p : peers) {
        if (p < 0 || p >= checkpoint::MAX_PEERS || p == func_id || presence.live(p)) {
            stall.armed = false;
            return;
        }
    }

    int cnt_restore = checkpointer->get_cnt_restore();

    // rearm the tracker is it's not armed, the peer set has changed or we straddled a checkpoint
    auto now = std::chrono::steady_clock::now();
    if (!stall.armed || stall.cnt_restore != cnt_restore || stall.peers != peers) {
        stall = {cnt_restore, peers, now, true, false};
        return;
    }

    if (stall.triggered)
        return;

    if (now - stall.since >= std::chrono::milliseconds(SELF_STOP_TIMEOUT_MS)) {
        BOOST_LOG_TRIVIAL(warning) << "waiting_on(): none of the " << peers.size() << " awaited peers live for "
                                   << SELF_STOP_TIMEOUT_MS << "ms (cnt_restore " << cnt_restore
                                   << "), triggering self stop";
        stall.triggered = true;
        checkpointer->self_stop(cnt_restore);
    }
}

void FMI::Comm::RedisCheckpoint::restore_fn() {
    double start_ms = common::now_monotonic_ms();
    context = redisConnect(hostname.c_str(), port);
    account_query(start_ms);
    if (context == nullptr || context->err) {
        if (context) {
            BOOST_LOG_TRIVIAL(error) << "Error when connecting to Redis: " << context->errstr;
        } else {
            BOOST_LOG_TRIVIAL(error) << "Allocating Redis context not possible";
        }
    }

    shutdown.store(false);
    presence.reset(); // repopulated from get_entries
    coordinator = std::make_unique<EtcdCoordinator>(etcd_host, etcd_port, getenv("job_id"));
    etcd_thread = std::thread(&RedisCheckpoint::handle_etcd, this);
}

void FMI::Comm::RedisCheckpoint::handle_etcd() {
    // Read current state before watching so we don't miss peers that registered
    // while we were checkpointed.
    std::vector<Entry> entries;
    for (int attempt = 1;; attempt++) {
        try {
            entries = coordinator->get_entries();
            break;
        } catch (const std::exception &e) {
            if (attempt == GET_ENTRIES_RETRIES) {
                BOOST_LOG_TRIVIAL(error) << "handle_etcd(): get_entries failed after retries: " << e.what();
                return;
            }
            BOOST_LOG_TRIVIAL(warning) << "handle_etcd(): get_entries failed (attempt " << attempt
                                       << "): " << e.what();
            std::this_thread::sleep_for(std::chrono::milliseconds(ETCD_POLL_MS));
        }
    }

    for (const auto &entry : entries) {
        if (entry.func_id == func_id)
            continue;
        process({entry.reachable() ? EventType::ADVERTISE_CONN : EventType::ADVERTISE_START, entry});
    }

    coordinator->start_watch();

    while (!shutdown.load()) {
        auto ev = coordinator->next_event(ETCD_POLL_MS);
        if (!ev)
            continue;

        if (ev->entry.func_id == func_id)
            continue;

        process(*ev);
    }

    BOOST_LOG_TRIVIAL(info) << "handle_etcd(): Thread stopped handling etcd";
}

void FMI::Comm::RedisCheckpoint::advertised_start(const Entry &entry) {
    BOOST_LOG_TRIVIAL(info) << "advertised_start(): peer " << entry.func_id
                            << " advertised to start (cnt_restore " << entry.cnt_restore << ")";
    presence.set(entry.func_id, true);
}

void FMI::Comm::RedisCheckpoint::advertised_conn(const Entry &entry) {
    BOOST_LOG_TRIVIAL(info) << "advertised_conn(): peer " << entry.func_id << " reachable at " << *entry.address
                            << ":" << *entry.port << " (cnt_restore " << entry.cnt_restore << ")";
    presence.set(entry.func_id, true);
}

void FMI::Comm::RedisCheckpoint::advertised_leave(const Entry &entry) {
    BOOST_LOG_TRIVIAL(info) << "advertised_leave(): peer " << entry.func_id << " left";
    presence.set(entry.func_id, false);
}
