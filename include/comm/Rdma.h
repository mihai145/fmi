#ifndef FMI_RDMA_H
#define FMI_RDMA_H

#include <atomic>
#include <condition_variable>
#include <rdmalib/rdmalib.hpp>
#include <netinet/in.h>
#include <thread>

#include "PeerToPeer.h"

namespace FMI::Comm
{
    struct PassiveConnection;
    struct ActiveConnection;

    /*
     * Data path: RDMA for message passing.
     * Control path: TCPunch for exchanging IP and RDMA port information.
     */
    class Rdma : public PeerToPeer
    {
    public:
        explicit Rdma(std::map<std::string, std::string> params);
        ~Rdma();

        void send_object(channel_data buf, Utils::peer_num rcpt_id) override;

        void recv_object(channel_data buf, Utils::peer_num sender_id) override;

        double get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override
        {
            return -1.; // not relevant
        }

        double get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override
        {
            return -1; // not relevant
        }

    private:
        // TCPunch connection settings
        struct TCPunchConn
        {
            std::string hostname;
            int port;
            int max_timeout;
        } tcpunch;

        // Rdma server
        std::atomic<bool> shutdown;
        int rdma_listen_port;
        std::string local_ip;
        std::unique_ptr<rdmalib::RDMAPassive> listener;
        std::thread listener_thread;

        std::mutex passive_map_mutex;
        std::condition_variable passive_map_cv;
        std::map<int, PassiveConnection> passive_connections;
        std::map<int, ActiveConnection> active_connections;

        // Communication protocol
        bool use_preallocated_buffers = false;

        void check_connection(int partner_id);
        void initialize_rdma_server();
        void listen_rdma();

        void connect_with_partner(int partner_id);
        void set_socket_options(int partner_socket);

        struct RdmaConnInfo
        {
            char ip[INET_ADDRSTRLEN];
            uint32_t rdma_port;
        };
        void send_rdma_conn_info(int partner_socket);
        void recv_rdma_conn_info(int partner_socket, RdmaConnInfo *conn_info);

        void insert_passive_connection(int partner_id, rdmalib::Connection *conn);
        void remove_passive_connection(int partner_id);

        void initialize_active_connection(int partner_id, RdmaConnInfo &conn_info);
    };

    struct PassiveConnection
    {
        ibv_pd *pd;
        std::unique_ptr<rdmalib::Connection> conn;
        rdmalib::Buffer<rdmalib::RemoteBuffer> rbuf_info;
        rdmalib::Buffer<char> preallocated;

        PassiveConnection(ibv_pd *pd, rdmalib::Connection *conn, bool preallocate);
    };

    struct ActiveConnection
    {
        rdmalib::RDMAActive active;
        rdmalib::Buffer<rdmalib::RemoteBuffer> rbuf_info;
        rdmalib::Buffer<char> preallocated;

        ActiveConnection(const std::string &ip, int port, bool preallocate);
        ActiveConnection(ActiveConnection &&obj);

        void post_recv_rbuf_info();
    };
}

#endif // FMI_RDMA_H
