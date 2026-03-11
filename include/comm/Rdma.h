#ifndef FMI_RDMA_H
#define FMI_RDMA_H

#include <atomic>
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
        std::map<int, PassiveConnection> passive_connections;
        std::map<int, ActiveConnection> active_connections;

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

        void initialize_passive_connection(int partner_id, rdmalib::Connection *conn);
        void remove_passive_connection(int partner_id);

        void initialize_active_connection(int partner_id, RdmaConnInfo &conn_info);
    };

    struct PassiveConnection
    {
        ibv_pd *pd;
        std::unique_ptr<rdmalib::Connection> conn;
        rdmalib::Buffer<uint32_t> rts_buffer;
        rdmalib::Buffer<uint32_t> wimm_buffer;
        rdmalib::Buffer<rdmalib::RemoteBuffer> buf_info_buffer;

        PassiveConnection(ibv_pd *pd, rdmalib::Connection *conn);

        void post_recv_rts();
        void post_recv_wimm();
    };

    struct ActiveConnection
    {
        rdmalib::RDMAActive active;
        rdmalib::Buffer<uint32_t> len_buffer;
        rdmalib::Buffer<rdmalib::RemoteBuffer> rbuf_info_buffer;

        ActiveConnection(const std::string &ip, int port);

        void post_recv_rbuf_info();
    };
}

#endif // FMI_RDMA_H
