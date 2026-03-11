#include "../../include/comm/Rdma.h"
#include <boost/log/trivial.hpp>
#include <ifaddrs.h>
#include <tcpunch.h>
#include <netinet/tcp.h>

constexpr int RDMA_LISTEN_PORT_OFFSET = 11005;
constexpr int RDMA_LISTEN_PORT_RANGE = 10000;
constexpr uint32_t RDMA_WRITE_WITH_IMMEDIATE_CT = 42;

FMI::Comm::Rdma::Rdma(std::map<std::string, std::string> params) : shutdown{false}, local_ip{"localhost"}, rdma_listen_port{-1}
{
    tcpunch.hostname = params["host"];
    tcpunch.port = std::stoi(params["port"]);
    tcpunch.max_timeout = std::stoi(params["max_timeout"]);

    // get local ip
    struct ifaddrs *ifaddr, *ifa;
    char ip[INET_ADDRSTRLEN];

    if (getifaddrs(&ifaddr) == -1)
        throw std::runtime_error("Could not get local ip");

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next)
    {
        if (!ifa->ifa_addr)
            continue;
        if (ifa->ifa_addr->sa_family != AF_INET)
            continue; // IPv4 only

        struct sockaddr_in *sa = (struct sockaddr_in *)ifa->ifa_addr;
        inet_ntop(AF_INET, &sa->sin_addr, ip, sizeof(ip));

        if (sa->sin_addr.s_addr != htonl(INADDR_LOOPBACK))
            break;
    }

    freeifaddrs(ifaddr);

    local_ip = std::string(ip);
    if (local_ip == "localhost")
        throw std::runtime_error("Could not get local ip, got localhost");
    else
        BOOST_LOG_TRIVIAL(info) << "Local ip is " << local_ip;
}

FMI::Comm::Rdma::~Rdma()
{
    shutdown.store(true);
    if (listener_thread.joinable())
        listener_thread.join();
}

void FMI::Comm::Rdma::send_object(channel_data buf, Utils::peer_num rcpt_id)
{
    /*
     * 1) Check connection
     * 2) Post recv for raddr, rkey (on active conn)
     * 3) Send len (on active conn)
     * 4) Poll for send completion
     * 5) Poll for recv completion
     * 6) Register mem buffer for RDMA write
     * 7) RDMA write with immediate
     * 8) Poll for completion
     * 9) Deregister mem buffer
     */

    check_connection(rcpt_id);

    ActiveConnection &active_conn = active_connections.at(rcpt_id);
    rdmalib::Connection &conn = active_conn.active.connection();

    // post recv for buffer information
    active_conn.post_recv_rbuf_info();

    // send length
    active_conn.len_buffer[0] = buf.len;
    conn.post_send(active_conn.len_buffer);

    // poll for send completion
    auto [wc_send, ret_send] = conn.poll_wc(rdmalib::QueueType::SEND, true, 1);
    if (wc_send->status == IBV_WC_SUCCESS)
    {
        BOOST_LOG_TRIVIAL(info) << peer_id << ": sent length to " << rcpt_id;
    }
    else
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": error sending length to " << rcpt_id << " " << ibv_wc_status_str(wc_send->status);
    }

    // poll for recv completion
    auto [wc_recv, ret_recv] = conn.poll_wc(rdmalib::QueueType::RECV, true, 1);
    auto [raddr, rkey, rsize] = active_conn.rbuf_info_buffer.data()[0];
    if (wc_recv->status == IBV_WC_SUCCESS)
    {
        BOOST_LOG_TRIVIAL(info) << peer_id << ": received from " << rcpt_id << " raddr " << raddr << " rkey " << rkey;
    }
    else
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": error receiving rbuf_info from " << rcpt_id << " " << ibv_wc_status_str(wc_recv->status);
    }

    // register mem buffer for RDMA write
    rdmalib::Buffer<char> rdma_buf(buf.buf, buf.len);
    rdma_buf.register_memory(active_conn.active.pd(), IBV_ACCESS_LOCAL_WRITE);

    // RDMA write with immediate
    conn.post_write(rdma_buf, rdmalib::RemoteBuffer{raddr, rkey, (uint32_t)buf.len}, RDMA_WRITE_WITH_IMMEDIATE_CT);

    // Poll for write completion
    auto [wc_wimm, ret_wimm] = conn.poll_wc(rdmalib::QueueType::SEND, true, 1);
    if (wc_wimm->status == IBV_WC_SUCCESS)
    {
        BOOST_LOG_TRIVIAL(info) << peer_id << ": wrote-with-immediate to " << rcpt_id;
    }
    else
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": error writing-with-immediate to " << rcpt_id << " " << ibv_wc_status_str(wc_wimm->status);
    }
}

void FMI::Comm::Rdma::recv_object(channel_data buf, Utils::peer_num sender_id)
{
    /*
     * 1) Check connection
     * ...2) Already posted RTS recv
     * 2) Poll for recv completion (on passive conn)
     * 3) Post 2 recvs (write with immediate + next RTS send_object)
     * 4) Register buffer memory, reply with rbuf_info
     * 5) Poll recv completion
     * 6) Deregister mem buffer (in Buffer destructor)
     */

    check_connection(sender_id);

    PassiveConnection &passive_conn = passive_connections.at(sender_id);

    // wait for RTS
    auto [wc_rts, ret_rts] = passive_conn.conn.get()->poll_wc(rdmalib::QueueType::RECV, true, 1);
    if (wc_rts->status == IBV_WC_SUCCESS)
    {
        BOOST_LOG_TRIVIAL(info) << peer_id << ": received length " << passive_conn.rts_buffer[0] << " from " << sender_id;
    }
    else
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": error receiving length from " << sender_id << " " << ibv_wc_status_str(wc_rts->status);
    }

    uint32_t rcvd_len = passive_conn.rts_buffer[0];
    if (rcvd_len != buf.len)
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": buffer lengths do not match - expected " << buf.len << " got " << rcvd_len;
    }

    // post receives for Write-with-immediate and next RTS
    passive_conn.post_recv_wimm();
    passive_conn.post_recv_rts();

    // register buffer
    rdmalib::Buffer<char> rdma_buffer(buf.buf, buf.len);
    rdma_buffer.register_memory(passive_conn.pd, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

    // send buffer information
    rdmalib::RemoteBuffer buf_info(rdma_buffer.address(), rdma_buffer.rkey(), rdma_buffer.size());
    passive_conn.buf_info_buffer[0] = buf_info;
    passive_conn.conn.get()->post_send(passive_conn.buf_info_buffer);

    auto [wc_send, ret_send] = passive_conn.conn.get()->poll_wc(rdmalib::QueueType::SEND, true, 1);
    if (wc_send->status == IBV_WC_SUCCESS)
    {
        BOOST_LOG_TRIVIAL(info) << peer_id << ": sent buffer info to " << sender_id << " raddr " << buf_info.addr << " rkey " << buf_info.rkey;
    }
    else
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": failed to send buffer info to " << sender_id << " " << ibv_wc_status_str(wc_send->status);
    }

    // wait for Write-with-immediate
    auto [wc_wimm, ret_wimm] = passive_conn.conn.get()->poll_wc(rdmalib::QueueType::RECV, true, 1);
    if (wc_wimm->status == IBV_WC_SUCCESS)
    {
        BOOST_LOG_TRIVIAL(info) << peer_id << ": successfully received write-with-immediate from " << sender_id;
    }
    else
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": failed to receive write-with-immediate from " << sender_id << " " << ibv_wc_status_str(wc_wimm->status);
    }

    uint32_t imm_data = ntohl(wc_wimm->imm_data);
    if (imm_data != RDMA_WRITE_WITH_IMMEDIATE_CT)
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": immediate does not match for sender " << sender_id << " - expected " << RDMA_WRITE_WITH_IMMEDIATE_CT << " got " << imm_data;
    }
}

void FMI::Comm::Rdma::initialize_rdma_server()
{
    int recv_buf_size = 2; // RTS + WriteWithImmediate

    // TODO: ports will be assigned by coordinator
    rdma_listen_port = RDMA_LISTEN_PORT_OFFSET + peer_id;
    listener = std::make_unique<rdmalib::RDMAPassive>(
        local_ip, rdma_listen_port, recv_buf_size, true);

    listener_thread = std::thread(&Rdma::listen_rdma, this);
}

void FMI::Comm::Rdma::listen_rdma()
{
    while (!shutdown.load())
    {
        bool result = listener->nonblocking_poll_events();
        if (!result)
            continue;

        auto [conn, conn_status] = listener->poll_events();
        if (conn == nullptr)
        {
            BOOST_LOG_TRIVIAL(error) << "Failed connection creation";
            continue;
        }

        rdmalib::PrivateData private_data{conn->private_data()};
        int partner_id = private_data.secret();

        if (conn_status == rdmalib::ConnectionStatus::DISCONNECTED)
        {
            BOOST_LOG_TRIVIAL(info) << peer_id << ": partner with id " << partner_id << " disconnected";
            remove_passive_connection(partner_id);
        }
        else if (conn_status == rdmalib::ConnectionStatus::REQUESTED)
        {
            BOOST_LOG_TRIVIAL(info) << peer_id << ": partner with id " << partner_id << " requested connection";
            initialize_passive_connection(partner_id, conn);
        }
        else if (conn_status == rdmalib::ConnectionStatus::ESTABLISHED)
        {
            BOOST_LOG_TRIVIAL(info) << peer_id << ": partner with id " << partner_id << " connected";
        }
    }

    BOOST_LOG_TRIVIAL(info) << "Listener thread stops listening for rdmacm events";
}

void FMI::Comm::Rdma::initialize_passive_connection(int partner_id, rdmalib::Connection *conn)
{
    std::lock_guard<std::mutex> lock(passive_map_mutex);
    passive_connections.emplace(std::piecewise_construct, std::forward_as_tuple(partner_id), std::forward_as_tuple(listener->pd(), conn));
    passive_connections.at(partner_id).post_recv_rts();
    listener->accept(conn);
}

void FMI::Comm::Rdma::remove_passive_connection(int partner_id)
{
    std::lock_guard<std::mutex> lock(passive_map_mutex);
    // passive_connections[partner_id].conn->close();
    passive_connections.erase(partner_id);
}

void FMI::Comm::Rdma::check_connection(int partner_id)
{
    if (rdma_listen_port == -1)
        initialize_rdma_server();

    if (active_connections.count(partner_id) == 0)
        connect_with_partner(partner_id);

    // passive connections are handled by listener thread
    int has_passive_connection = 0;
    do
    {
        std::lock_guard<std::mutex> lock(passive_map_mutex);
        has_passive_connection = passive_connections.count(partner_id);
    } while (!has_passive_connection);
}

void FMI::Comm::Rdma::connect_with_partner(int partner_id)
{
    /*
  int partner_socket = -1;

  try
  {
      int min_peer_id = std::min((int)peer_id, partner_id);
      int max_peer_id = std::max((int)peer_id, partner_id);
      std::string pairing_name = comm_name + std::to_string(min_peer_id) + "_" + std::to_string(max_peer_id);

      partner_socket = pair(pairing_name, tcpunch.hostname, tcpunch.port, tcpunch.max_timeout);
      set_socket_options(partner_socket);
  }
  catch (Timeout)
  {
      throw Utils::Timeout();
  }

  BOOST_LOG_TRIVIAL(info) << peer_id << ": connected via TCP with " << partner_id;

  RdmaConnInfo conn_info;
  if (peer_id < partner_id)
  {
      send_rdma_conn_info(partner_socket);
      recv_rdma_conn_info(partner_socket, &conn_info);
  }
  else
  {
      recv_rdma_conn_info(partner_socket, &conn_info);
      send_rdma_conn_info(partner_socket);
  }
  */

    // TODO: this makes the strong assumption that all functions are on the same machine and that rdma ports can be allocated consecutively
    // This will be changed when implementing a coordinator for gap scheduling
    RdmaConnInfo conn_info;
    snprintf(conn_info.ip, sizeof(conn_info.ip), "%s", local_ip.c_str());
    conn_info.rdma_port = RDMA_LISTEN_PORT_OFFSET + partner_id;

    initialize_active_connection(partner_id, conn_info);

    // close(partner_socket);
}

void FMI::Comm::Rdma::set_socket_options(int partner_socket)
{
    struct timeval timeout;
    timeout.tv_sec = tcpunch.max_timeout / 1000;
    timeout.tv_usec = (tcpunch.max_timeout % 1000) * 1000;
    setsockopt(partner_socket, SOL_SOCKET, SO_RCVTIMEO, (const char *)&timeout, sizeof timeout);
    setsockopt(partner_socket, SOL_SOCKET, SO_SNDTIMEO, (const char *)&timeout, sizeof timeout);
    // Disable Nagle algorithm to avoid 40ms TCP ack delays
    int one = 1;
// SOL_TCP not defined on macOS
#if !defined(SOL_TCP) && defined(IPPROTO_TCP)
#define SOL_TCP IPPROTO_TCP
#endif
    setsockopt(partner_socket, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
}

void FMI::Comm::Rdma::send_rdma_conn_info(int partner_socket)
{
    RdmaConnInfo conn_info;
    snprintf(conn_info.ip, sizeof(conn_info.ip), "%s", local_ip.c_str());
    conn_info.rdma_port = rdma_listen_port;

    ssize_t sent = ::send(partner_socket, &conn_info, sizeof(RdmaConnInfo), 0);
    if (sent != sizeof(RdmaConnInfo))
    {
        if (errno == EAGAIN)
        {
            throw Utils::Timeout();
        }
        BOOST_LOG_TRIVIAL(error) << peer_id << ": error when sending conn_info " << strerror(errno);
    }
}

void FMI::Comm::Rdma::recv_rdma_conn_info(int partner_socket, RdmaConnInfo *conn_info)
{
    ssize_t received = ::recv(partner_socket, conn_info, sizeof(RdmaConnInfo), MSG_WAITALL);
    if (received != sizeof(RdmaConnInfo))
    {
        if (errno == EAGAIN)
        {
            throw Utils::Timeout();
        }
        BOOST_LOG_TRIVIAL(error) << peer_id << ": error when receiving conn_info " << strerror(errno);
    }
}

void FMI::Comm::Rdma::initialize_active_connection(int partner_id, RdmaConnInfo &conn_info)
{
    active_connections.emplace(std::piecewise_construct, std::forward_as_tuple(partner_id), std::forward_as_tuple(conn_info.ip, conn_info.rdma_port));

    BOOST_LOG_TRIVIAL(info) << peer_id << ": connecting to partner " << partner_id << " at ip " << conn_info.ip << " and rdma port " << conn_info.rdma_port;

    if (!active_connections.at(partner_id).active.connect(peer_id))
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": could not connect to partner " << partner_id;
    }
    else
    {
        BOOST_LOG_TRIVIAL(info) << peer_id << ": connected to partner " << partner_id;
    }
}

FMI::Comm::PassiveConnection::PassiveConnection(ibv_pd *pd, rdmalib::Connection *conn) : pd{pd}, conn{conn}, rts_buffer{1}, wimm_buffer{1}, buf_info_buffer{1}
{
    rts_buffer.register_memory(pd, IBV_ACCESS_LOCAL_WRITE);
    wimm_buffer.register_memory(pd, IBV_ACCESS_LOCAL_WRITE);
    buf_info_buffer.register_memory(pd, IBV_ACCESS_LOCAL_WRITE);
}

void FMI::Comm::PassiveConnection::post_recv_rts()
{
    conn->post_recv(rts_buffer);
}

void FMI::Comm::PassiveConnection::post_recv_wimm()
{
    conn->post_recv(wimm_buffer);
}

FMI::Comm::ActiveConnection::ActiveConnection(const std::string &ip, int port) : active{ip, port}, rbuf_info_buffer{1}, len_buffer{1}
{
    active.allocate();
    rbuf_info_buffer.register_memory(active.pd(), IBV_ACCESS_LOCAL_WRITE);
    len_buffer.register_memory(active.pd(), IBV_ACCESS_LOCAL_WRITE);
}

void FMI::Comm::ActiveConnection::post_recv_rbuf_info()
{
    active.connection().post_recv(rbuf_info_buffer);
}
