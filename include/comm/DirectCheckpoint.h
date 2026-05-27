#ifndef FMI_DIRECT_CHECKPOINT_H
#define FMI_DIRECT_CHECKPOINT_H

#include "EtcdCoordinator.h"
#include "PeerToPeer.h"
#include "checkpoint_v2.hpp"

#include <deque>

namespace FMI::Comm {

    class DirectCheckpoint : public PeerToPeer {
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

        void teardown_fn();
        void restore_fn();

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

        int bind_and_listen();
        bool connect_to(int peer_id, const std::string &address, int port);
        void drain_connection(int peer_id);
    };
} // namespace FMI::Comm

#endif // FMI_DIRECT_CHECKPOINT_H
