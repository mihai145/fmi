#include "../../include/comm/Rdma.h"
#include <boost/log/trivial.hpp>
#include <ifaddrs.h>
#include <tcpunch.h>
#include <netinet/tcp.h>

constexpr int RDMA_LISTEN_PORT_OFFSET = 11005;
constexpr int RDMA_LISTEN_PORT_RANGE = 10000;
constexpr std::chrono::seconds RDMA_CONNECT_TIMEOUT = std::chrono::seconds(5);
constexpr std::chrono::milliseconds RDMA_CONNECT_BACKOFF = std::chrono::milliseconds(10);
constexpr uint32_t RDMA_WRITE_WITH_IMMEDIATE_CT = 42;
constexpr size_t RDMA_PREALLOCATED_SIZE = 10 * 1024 * 1024; // 10MB

FMI::Comm::Rdma::Rdma(std::map<std::string, std::string> params) : shutdown{false}, local_ip{"localhost"}, rdma_listen_port{-1}
{
    tcpunch.hostname = params["host"];
    tcpunch.port = std::stoi(params["port"]);
    tcpunch.max_timeout = std::stoi(params["max_timeout"]);

    if (params["preallocate_buffers"] == "true")
        use_preallocated_buffers = true;
    else
        use_preallocated_buffers = false;
    BOOST_LOG_TRIVIAL(info) << "Preallocate buffers: " << params["preallocate_buffers"];

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
     1) Check connection
     2) Poll recv for rbuf_info (on active conn)
     3) Post recv for rbuf_info (for next send)
     4) Write with immediate
     5) Poll for completion
     */

    check_connection(rcpt_id);

    ActiveConnection &active_conn = active_connections.at(rcpt_id);
    rdmalib::Connection &conn = active_conn.active.connection();

    // Poll recv for rbuf_info
    auto [wc_recv, ret_recv] = conn.poll_wc(rdmalib::QueueType::RECV, true, 1);
    if (ret_recv != 1 || wc_recv->status != IBV_WC_SUCCESS)
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": failed to poll for recv rbuf_info completion " << ibv_wc_status_str(wc_recv->status);
        throw std::runtime_error("Failed poll for recv");
    }

    rdmalib::RemoteBuffer rbuf = active_conn.rbuf_info[0];
    BOOST_LOG_TRIVIAL(info) << peer_id << ": received rbuf info from " << rcpt_id << ": raddr=" << rbuf.addr << " rkey=" << rbuf.rkey << " size=" << rbuf.size;

    // Post recv for rbuf_info
    active_conn.post_recv_rbuf_info();

    // Write with immediate
    std::unique_ptr<rdmalib::Buffer<char>> send_buf = nullptr;
    if (use_preallocated_buffers)
    {
        memcpy(active_conn.preallocated.data(), buf.buf, buf.len);

        rdmalib::ScatterGatherElement sge;
        sge.add(active_conn.preallocated, buf.len, 0);

        conn.post_write(std::move(sge), rbuf, RDMA_WRITE_WITH_IMMEDIATE_CT);
    }
    else
    {
        send_buf = std::make_unique<rdmalib::Buffer<char>>(buf.buf, buf.len);
        send_buf->register_memory(active_conn.active.pd(), IBV_ACCESS_LOCAL_WRITE);

        conn.post_write(*send_buf, rbuf, RDMA_WRITE_WITH_IMMEDIATE_CT);
    }

    // Poll for completion
    auto [wc_wimm, ret_wimm] = conn.poll_wc(rdmalib::QueueType::SEND, true, 1);
    if (ret_wimm != 1 || wc_wimm->status != IBV_WC_SUCCESS)
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": failed polling for wc_wimm " << ibv_wc_status_str(wc_wimm->status);
        throw std::runtime_error("Failed polling for wc_wimm");
    }

    BOOST_LOG_TRIVIAL(info) << peer_id << ": sent data to " << rcpt_id;
    if (send_buf != nullptr)
        send_buf.reset();
}

void FMI::Comm::Rdma::recv_object(channel_data buf, Utils::peer_num sender_id)
{
    /*
     1) Check connection
     2) Post recv for write with immediate
     3) Send rbuf_info
     4) Poll for send and recv completions
     */

    check_connection(sender_id);

    passive_map_mutex.lock();
    PassiveConnection &passive_conn = passive_connections.at(sender_id);
    passive_map_mutex.unlock();

    rdmalib::Connection *conn = passive_conn.conn.get();

    // Post recv for write with immediate
    std::unique_ptr<rdmalib::Buffer<char>> recv_buf = nullptr;
    if (use_preallocated_buffers)
    {
        rdmalib::ScatterGatherElement sge;
        sge.add(passive_conn.preallocated, buf.len, 0);
        conn->post_recv(std::move(sge));

        passive_conn.rbuf_info[0] = {
            passive_conn.preallocated.address(),
            passive_conn.preallocated.rkey(),
            (uint32_t)buf.len};
    }
    else
    {
        recv_buf = std::make_unique<rdmalib::Buffer<char>>(buf.buf, buf.len);
        recv_buf->register_memory(passive_conn.pd, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
        conn->post_recv(*recv_buf);

        passive_conn.rbuf_info[0] = {
            recv_buf->address(),
            recv_buf->rkey(),
            (uint32_t)buf.len};
    }

    // Send rbuf_info
    conn->post_send(passive_conn.rbuf_info);

    // Poll for send and recv completions
    auto [wc_send, ret_send] = conn->poll_wc(rdmalib::QueueType::SEND, true, 1);
    if (ret_send != 1 || wc_send->status != IBV_WC_SUCCESS)
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": failed polling for wc_send " << ibv_wc_status_str(wc_send->status);
        throw std::runtime_error("Failed polling for wc_send");
    }

    BOOST_LOG_TRIVIAL(info) << peer_id << ": sent rbuf info to " << sender_id
                            << ": raddr=" << passive_conn.rbuf_info[0].addr
                            << " rkey=" << passive_conn.rbuf_info[0].rkey
                            << " size=" << passive_conn.rbuf_info[0].size;

    auto [wc_wimm, ret_wimm] = conn->poll_wc(rdmalib::QueueType::RECV, true, 1);
    if (ret_wimm != 1 || wc_wimm->status != IBV_WC_SUCCESS)
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": failed polling for wc_wimm " << ibv_wc_status_str(wc_wimm->status);
        throw std::runtime_error("Failed polling for wc_wimm");
    }

    uint32_t imm_data = ntohl(wc_wimm->imm_data);
    if (imm_data != RDMA_WRITE_WITH_IMMEDIATE_CT)
    {
        BOOST_LOG_TRIVIAL(error) << peer_id << ": immediate does not match, expected " << RDMA_WRITE_WITH_IMMEDIATE_CT << " got " << imm_data;
        throw std::runtime_error("Immediate does not match");
    }

    if (use_preallocated_buffers)
        memcpy(buf.buf, passive_conn.preallocated.data(), buf.len);
    else
        recv_buf.reset();

    BOOST_LOG_TRIVIAL(info) << peer_id << ": received data from " << sender_id;
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
            listener->accept(conn);
        }
        else if (conn_status == rdmalib::ConnectionStatus::ESTABLISHED)
        {
            BOOST_LOG_TRIVIAL(info) << peer_id << ": partner with id " << partner_id << " connected";
            insert_passive_connection(partner_id, conn);
        }
    }

    BOOST_LOG_TRIVIAL(info) << "Listener thread stops listening for rdmacm events";
}

void FMI::Comm::Rdma::insert_passive_connection(int partner_id, rdmalib::Connection *conn)
{
    std::lock_guard<std::mutex> lock(passive_map_mutex);
    passive_connections.emplace(std::piecewise_construct, std::forward_as_tuple(partner_id), std::forward_as_tuple(listener->pd(), conn, use_preallocated_buffers));
    passive_map_cv.notify_one();
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
    do
    {
        std::unique_lock<std::mutex> lock(passive_map_mutex);
        if (passive_connections.count(partner_id))
            break;
        else
            passive_map_cv.wait(lock);
    } while (true);
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
    BOOST_LOG_TRIVIAL(info) << peer_id << ": connecting to partner " << partner_id << " at ip " << conn_info.ip << " and rdma port " << conn_info.rdma_port;

    auto start = std::chrono::steady_clock::now();
    for (int i = 0;; i++)
    {
        ActiveConnection conn(conn_info.ip, conn_info.rdma_port, use_preallocated_buffers);

        if (conn.active.connect(peer_id))
        {
            active_connections.emplace(partner_id, std::move(conn));
            BOOST_LOG_TRIVIAL(info) << peer_id << ": connected to partner " << partner_id << " after " << i << " retries";
            return;
        }

        BOOST_LOG_TRIVIAL(warning) << peer_id << ": could not connect to partner " << partner_id << ", retrying...";
        std::this_thread::sleep_for(RDMA_CONNECT_BACKOFF);

        auto now = std::chrono::steady_clock::now();
        if (now - start > RDMA_CONNECT_TIMEOUT) break;
    }

    BOOST_LOG_TRIVIAL(error) << peer_id << ": could not connect to partner " << partner_id;
    throw std::runtime_error("could not connect to partner");
}

FMI::Comm::PassiveConnection::PassiveConnection(ibv_pd *pd, rdmalib::Connection *conn, bool preallocate) : pd{pd}, conn{conn}, rbuf_info{1}, preallocated{preallocate ? RDMA_PREALLOCATED_SIZE : 0}
{
    rbuf_info.register_memory(pd, IBV_ACCESS_LOCAL_WRITE);

    if (preallocate)
        preallocated.register_memory(pd, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
}

FMI::Comm::ActiveConnection::ActiveConnection(const std::string &ip, int port, bool preallocate) : active{ip, port}, rbuf_info{1}, preallocated{preallocate ? RDMA_PREALLOCATED_SIZE : 0}
{
    active.allocate();
    rbuf_info.register_memory(active.pd(), IBV_ACCESS_LOCAL_WRITE);

    if (preallocate)
        preallocated.register_memory(active.pd(), IBV_ACCESS_LOCAL_WRITE);

    post_recv_rbuf_info();
}

FMI::Comm::ActiveConnection::ActiveConnection(ActiveConnection &&obj) : rbuf_info{std::move(obj.rbuf_info)}, preallocated{std::move(obj.preallocated)}
{
    active = std::move(obj.active);
}

void FMI::Comm::ActiveConnection::post_recv_rbuf_info()
{
    active.connection().post_recv(rbuf_info);
}
