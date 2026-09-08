#ifndef FMI_REDIS_CHECKPOINT_H
#define FMI_REDIS_CHECKPOINT_H

#include "ClientServer.h"
#include "EtcdCoordinator.h"
#include <atomic>
#include <chrono>
#include <hiredis/hiredis.h>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace FMI::Comm {
    //! Checkpointable channel that uses Redis with the Hiredis client library
    //! as storage backend.
    class RedisCheckpoint : public ClientServer, public MembershipHandler {
      public:
        explicit RedisCheckpoint(
            std::map<std::string, std::string> params,
            std::map<std::string, std::string> model_params);

        ~RedisCheckpoint();

        void upload_object(channel_data buf, std::string name) override;

        bool download_object(channel_data buf, std::string name) override;

        void delete_object(std::string name) override;

        std::vector<std::string> get_object_names() override;

        double get_latency(Utils::peer_num producer, Utils::peer_num consumer,
                           std::size_t size_in_bytes) override;

        double get_price(Utils::peer_num producer, Utils::peer_num consumer,
                         std::size_t size_in_bytes) override;

      private:
        std::string hostname;
        int port;
        redisContext *context;

        // etcd membership tracking
        std::string etcd_host;
        int etcd_port;
        int func_id;
        std::unique_ptr<Coordinator> coordinator;
        std::atomic<bool> shutdown{false};
        std::thread etcd_thread;

        void handle_etcd();

        // membership callbacks, run on the etcd thread
        void advertised_start(const Entry &entry) override;
        void advertised_conn(const Entry &entry) override;
        void advertised_leave(const Entry &entry) override;

        // Peer presence tracking
        PeerPresence presence;

        // Self-stop when every peer we are waiting on stays dead too long
        struct StallTracker {
            int cnt_restore{-1};
            std::vector<int> peers;
            std::chrono::steady_clock::time_point since;
            bool armed{false};
            bool triggered{false};
        };
        StallTracker stall;

        void waiting_on(const std::vector<int> &peers) override;

        // Time blocked in the polling loops and time spent in Redis queries
        double wait_ms{0};
        double redis_ms{0};
        unsigned long long redis_queries{0};

        struct WaitTracker {
            bool armed{false};
            int cnt_restore{-1};
            std::chrono::steady_clock::time_point since;
        };
        WaitTracker wait;

        void wait_tick(bool waiting);
        void account_query(double start_ms);
        void publish_wait();

        // Model params
        double bandwidth_single;
        double bandwidth_multiple;
        double overhead;
        double transfer_price;
        double instance_price;
        unsigned int requests_per_hour;
        bool include_infrastructure_costs;

        // teardown / restore functions for C/R
        void teardown_fn();
        void restore_fn();
    };
} // namespace FMI::Comm

#endif // FMI_REDIS_CHECKPOINT_H
