#include "../../include/comm/DirectNoHolepunch.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <boost/log/trivial.hpp>
#include <thread>

namespace
{
    constexpr int FMI_PORT_OFFSET = 20000;
    constexpr int FMI_PORT_RANGE = 20000;
    std::chrono::milliseconds GET_PEER_DETAILS_BACKOFF(1);
}

FMI::Comm::DirectNoHolepunch::DirectNoHolepunch(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params)
{
    listen_sock = -1;
    restore_fn();
}

FMI::Comm::DirectNoHolepunch::~DirectNoHolepunch()
{
    teardown_fn();
}

void FMI::Comm::DirectNoHolepunch::send_object(channel_data buf, Utils::peer_num rcpt_id)
{
    // blocking send all bytes to given peer
    check_socket(rcpt_id);

    BOOST_LOG_TRIVIAL(info) << peer_id << ": send object to " << rcpt_id;

    std::size_t total_sent = 0;
    while (total_sent < buf.len)
    {
        long sent = ::send(sockets[rcpt_id], (char *)buf.buf + total_sent, buf.len - total_sent, 0);
        if (sent == -1)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            BOOST_LOG_TRIVIAL(error) << peer_id << ": Error when sending: " << strerror(errno);
            return;
        }
        total_sent += sent;
    }
}

void FMI::Comm::DirectNoHolepunch::recv_object(channel_data buf, Utils::peer_num sender_id)
{
    // blocking recv all bytes from given peer
    check_socket(sender_id);

    BOOST_LOG_TRIVIAL(info) << peer_id << ": recv object from " << sender_id;

    std::size_t total_received = 0;
    while (total_received < buf.len)
    {
        long received = ::recv(sockets[sender_id], (char *)buf.buf + total_received, buf.len - total_received, 0);
        if (received == -1)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            BOOST_LOG_TRIVIAL(error) << peer_id << ": Error when receiving: " << strerror(errno);
            return;
        }
        if (received == 0)
        {
            BOOST_LOG_TRIVIAL(error) << peer_id << ": Peer " << sender_id << " closed connection";
            return;
        }
        total_received += received;
    }
}

void FMI::Comm::DirectNoHolepunch::check_socket(Utils::peer_num partner_id)
{
    if (sockets.empty())
        sockets = std::vector<int>(num_peers, -1);

    if (sockets[partner_id] != -1)
        return;

    while (peers[partner_id].port == -1)
    {
        BOOST_LOG_TRIVIAL(info) << peer_id << ": Unknown address for partner " << partner_id << ", getting peer details...";

        auto result = checkpointer.get_peer_details();
        if (std::holds_alternative<Error>(result))
        {
            BOOST_LOG_TRIVIAL(error) << peer_id << ": Failed to get peer details: " << get_error_message(result);
            throw std::runtime_error("Failed to get peer details");
        }

        peers = std::get<std::vector<checkpoint::peer_details>>(result);

        if (peers[partner_id].port == -1)
            std::this_thread::sleep_for(GET_PEER_DETAILS_BACKOFF);
    }

    if (peer_id < partner_id)
    {
        BOOST_LOG_TRIVIAL(info) << peer_id << ": Trying connect() to " << partner_id;

        struct sockaddr_in dest{};
        dest.sin_family = AF_INET;
        dest.sin_port = htons(peers[partner_id].port);
        inet_pton(AF_INET, peers[partner_id].ip, &dest.sin_addr);

        while (true)
        {
            sockets[partner_id] = ::socket(AF_INET, SOCK_STREAM, 0);
            if (::connect(sockets[partner_id], (struct sockaddr *)&dest, sizeof(dest)) == 0)
            {
                // send peer id so that partner can identify us
                if (::send(sockets[partner_id], &peer_id, sizeof(peer_id), 0) != sizeof(peer_id))
                {
                    BOOST_LOG_TRIVIAL(error) << peer_id << ": Could not send identification message to partner " << partner_id;
                    throw std::runtime_error("Could not send identification message to partner");
                }

                break;
            }
            BOOST_LOG_TRIVIAL(debug) << peer_id << ": Connect to peer " << partner_id << " failed, retrying...";
            ::close(sockets[partner_id]);
            std::this_thread::sleep_for(TCP_CONNECT_BACKOFF);
        }
    }
    else
    {
        BOOST_LOG_TRIVIAL(info) << peer_id << ": Trying accept() from " << partner_id;

        struct sockaddr_in src{};
        socklen_t src_len = sizeof(src);
        int new_socket = ::accept(listen_sock, (struct sockaddr *)&src, &src_len);
        if (new_socket < 0)
        {
            BOOST_LOG_TRIVIAL(error) << peer_id << ": Accept failed: " << strerror(errno);
            throw std::runtime_error("Accept failed");
        }

        // identify newly connected peer
        Utils::peer_num newly_connected_id;
        if (::recv(new_socket, &newly_connected_id, sizeof(newly_connected_id), MSG_WAITALL) != sizeof(newly_connected_id))
        {
            BOOST_LOG_TRIVIAL(error) << peer_id << ": Could not receive identification message from partner " << partner_id;
            throw std::runtime_error("Could not receive identification message from partner");
        }

        sockets[newly_connected_id] = new_socket;
        if (newly_connected_id != partner_id) // accept the connection we actually want
            check_socket(partner_id);
    }

    int one = 1;
#if !defined(SOL_TCP) && defined(IPPROTO_TCP)
#define SOL_TCP IPPROTO_TCP
#endif
    setsockopt(sockets[partner_id], SOL_TCP, TCP_NODELAY, &one, sizeof(one));
}

void FMI::Comm::DirectNoHolepunch::restore_fn()
{
    srand(time(nullptr));

    int own_id = checkpointer.get_own_id();
    BOOST_LOG_TRIVIAL(info) << own_id << ": intializing state, finding a port to listen to..";

    int port = -1;
    while (true)
    {
        // open TCP server socket
        listen_sock = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        port = FMI_PORT_OFFSET + rand() % FMI_PORT_RANGE;
        BOOST_LOG_TRIVIAL(info) << own_id << ": trying to bind to port " << port;

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        if (::bind(listen_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        {
            ::close(listen_sock);
            BOOST_LOG_TRIVIAL(error) << own_id << ": Failed to bind port " << port << ", retrying..";
            continue;
        }
        else
            BOOST_LOG_TRIVIAL(info) << own_id << ": bound to port " << port;

        if (::listen(listen_sock, num_peers) < 0)
        {
            ::close(listen_sock);
            BOOST_LOG_TRIVIAL(error) << own_id << ": Failed to listen to bound address, retrying...";
            continue;
        }
        else
            break;
    }

    BOOST_LOG_TRIVIAL(info) << own_id << ": Listening on port " << port;

    auto res_register = checkpointer.register_peer_details(port);
    if (std::holds_alternative<Error>(res_register))
    {
        BOOST_LOG_TRIVIAL(error) << own_id << ": Failed to register peer details: " << get_error_message(res_register);
        throw std::runtime_error("Failed to register peer details");
    }

    auto result = checkpointer.get_peer_details();
    if (std::holds_alternative<Error>(result))
    {
        BOOST_LOG_TRIVIAL(error) << own_id << ": Failed to get peer details: " << get_error_message(result);
        throw std::runtime_error("Failed to get peer details");
    }

    peers = std::get<std::vector<checkpoint::peer_details>>(result);
    BOOST_LOG_TRIVIAL(info) << own_id << ": initializing state, got " << peers.size() << " peers";

    BOOST_LOG_TRIVIAL(info) << own_id << ": restored state";
}

void FMI::Comm::DirectNoHolepunch::teardown_fn()
{
    BOOST_LOG_TRIVIAL(info) << peer_id << ": closing connections to peers and listening socket...";

    for (auto &s : sockets)
        if (s >= 0)
        {
            ::close(s);
            s = -1;
        }

    if (listen_sock >= 0)
    {
        ::close(listen_sock);
        listen_sock = -1;
    }

    BOOST_LOG_TRIVIAL(info) << peer_id << ": tearing down state complete";
}
