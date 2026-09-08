#ifndef FMI_DIRECT_CHECKPOINT_H
#define FMI_DIRECT_CHECKPOINT_H

#include "EtcdCoordinator.h"
#include "PeerToPeer.h"
#include "checkpoint_v2.hpp"

#include <chrono>
#include <deque>
#include <unordered_map>

namespace FMI::Comm {

    class DirectCheckpoint : public PeerToPeer, public MembershipHandler {
      public:
        explicit DirectCheckpoint(std::map<std::string, std::string> params,
                                  std::map<std::string, std::string> model_params);
        ~DirectCheckpoint();

        void send_object(channel_data buf, Utils::peer_num rcpt_id) override;
        void recv_object(channel_data buf, Utils::peer_num sender_id) override;

        double get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;
        double get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;

      private:
        std::string etcd_host;
        int etcd_port;
        int func_id;

        std::unique_ptr<checkpoint::Checkpointer> checkpointer;
        std::unique_ptr<Coordinator> coordinator;
        checkpoint::UninterruptibleContextState state{0}; // scheduler hint

        void teardown_fn();
        void restore_fn();
        void publish_wait();

        double wait_ms{0};
        double comm_ms{0};
        unsigned long long comm_calls{0};

        std::atomic<bool> shutdown{false};
        int listener_port{-1};
        int listener_fd{-1};

        std::thread listener_thread;
        std::thread etcd_thread;

        struct ConnectionInfo {
            int fd;
            std::string address;
            int port;
        };

        std::mutex connections_lock;
        std::unordered_map<int, ConnectionInfo> connections; // func_id -> connection info

        std::mutex recv_buffers_lock;
        std::unordered_map<int, std::deque<char>> recv_buffers; // bytes drained during teardown

        void handle_listener();
        void handle_etcd();

        // membership callbacks, run on the etcd thread
        void advertised_start(const Entry &entry) override;
        void advertised_conn(const Entry &entry) override;
        void advertised_leave(const Entry &entry) override;

        struct PendingConnect {
            std::string address;
            int port;
            std::chrono::steady_clock::time_point next_retry;
        };
        std::unordered_map<int, PendingConnect> pending_connects; // etcd thread

        // Peer presence tracking
        PeerPresence presence;

        // Self-stop when a peer we must talk to stays dead too long
        struct StallTracker {
            int cnt_restore{-1};
            int peer{-1};
            std::chrono::steady_clock::time_point since;
            bool armed{false};
            bool triggered{false};
        };
        StallTracker stall;

        void stall_reset() { stall.armed = false; }
        void stall_check(int peer, int cnt_restore);

        int bind_and_listen();
        bool connect_to(int peer_id, const std::string &address, int port);
        void drain_connection(int peer_id);
        void drain_all(const std::vector<int> &peer_ids);

        // hint state helpers
        void hint_send_wait(FMI::Utils::peer_num rcpt_id) {
          state.waiting_for[rcpt_id] = true;
          state.current_state = checkpoint::Send{(int)rcpt_id};
        }

        void hint_send_done(FMI::Utils::peer_num rcpt_id) {
          state.done_send[rcpt_id]++;
          state.waiting_for[rcpt_id] = false;
          state.current_state = checkpoint::CPUBound{};
        }

        void hint_recv_wait(FMI::Utils::peer_num sender_id) {
          state.waiting_for[sender_id] = true;
          state.current_state = checkpoint::Recv{(int)sender_id};
        }

        void hint_recv_done(FMI::Utils::peer_num sender_id) {
          state.done_recv[sender_id]++;
          state.waiting_for[sender_id] = false;
          state.current_state = checkpoint::CPUBound{};
        }
    };
} // namespace FMI::Comm

#endif // FMI_DIRECT_CHECKPOINT_H
