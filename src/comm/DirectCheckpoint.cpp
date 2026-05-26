#include "../../include/comm/DirectCheckpoint.h"

#include <arpa/inet.h>
#include <boost/log/trivial.hpp>
#include <cerrno>
#include <chrono>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {
    constexpr int RCVTIMEO_US = 5 * 1000;
    constexpr int DRAIN_RCVTIMEO_US = 500 * 1000;
    constexpr int LISTENER_POLL_MS = 100;
    constexpr int ETCD_POLL_MS = 200;
} // namespace

namespace FMI::Comm {

    DirectCheckpoint::DirectCheckpoint(std::map<std::string, std::string> params,
                                       std::map<std::string, std::string> model_params) {
        etcd_host = params["etcd_host"];
        etcd_port = std::stoi(params["etcd_port"]);
        func_id = std::stoi(getenv("func_id"));

        checkpoint::Checkpointer::setup_thread();
        restore_fn();

        checkpointer = std::make_unique<checkpoint::Checkpointer>([this] { teardown_fn(); }, [this] { restore_fn(); });
    }

    DirectCheckpoint::~DirectCheckpoint() {
        shutdown.store(true);
        if (listener_thread.joinable())
            listener_thread.join();
        if (etcd_thread.joinable())
            etcd_thread.join();
    }

    void DirectCheckpoint::send_object(channel_data buf, Utils::peer_num rcpt_id) {
        int sent = 0;

        while (sent < (int)buf.len) {
            bool need_sleep = false;
            {
                auto ctx = checkpointer->get_uninterruptible_context();

                // find connection
                int fd = -1;
                {
                    std::lock_guard<std::mutex> lock(connections_lock);
                    auto it = connections.find(rcpt_id);
                    if (it != connections.end())
                        fd = it->second.fd;
                }

                if (fd == -1) {
                    BOOST_LOG_TRIVIAL(info) << "send_object(): No connection to " << rcpt_id;
                    need_sleep = true;
                } else {
                    while (true) {
                        ssize_t n = ::send(fd, buf.buf + sent, buf.len - sent, MSG_NOSIGNAL);
                        if (n > 0) {
                            sent += n;
                            if (sent == (int)buf.len)
                                break;
                        } else if (n < 0 && errno == EAGAIN) {
                            continue;
                        } else {
                            BOOST_LOG_TRIVIAL(info)
                                << "send_object(): Got errno " << errno << " while sending data to " << rcpt_id;
                            std::lock_guard<std::mutex> lock(connections_lock);
                            connections.erase(rcpt_id);
                            ::close(fd);
                            break;
                        }
                    }
                }
            }

            if (need_sleep)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        BOOST_LOG_TRIVIAL(info) << "send_object(): Sent data to " << rcpt_id;
    }

    void DirectCheckpoint::recv_object(channel_data buf, Utils::peer_num sender_id) {
        int recvd = 0;

        while (recvd < (int)buf.len) {
            // Drain bytes saved during teardown first
            {
                std::lock_guard<std::mutex> lock(recv_buffers_lock);
                auto it = recv_buffers.find(sender_id);
                if (it != recv_buffers.end() && !it->second.empty()) {
                    auto &q = it->second;
                    int to_copy = std::min((int)q.size(), (int)buf.len - recvd);
                    std::copy(q.begin(), q.begin() + to_copy, buf.buf + recvd);
                    q.erase(q.begin(), q.begin() + to_copy);
                    recvd += to_copy;

                    BOOST_LOG_TRIVIAL(info) << "recv_object(): Drained " << to_copy << " bytes from " << sender_id;
                }
            }

            if (recvd == (int)buf.len)
                break;

            bool need_sleep = false;
            {
                auto ctx = checkpointer->get_uninterruptible_context();

                // find connection
                int fd = -1;
                {
                    std::lock_guard<std::mutex> lock(connections_lock);
                    auto it = connections.find(sender_id);
                    if (it != connections.end())
                        fd = it->second.fd;
                }

                if (fd == -1) {
                    BOOST_LOG_TRIVIAL(info) << "recv_object(): No connection to " << sender_id;
                    need_sleep = true;
                } else {
                    while (true) {
                        ssize_t n = ::recv(fd, buf.buf + recvd, buf.len - recvd, 0);
                        if (n > 0) {
                            recvd += n;
                            if (recvd == (int)buf.len)
                                break;
                        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                            break;
                        } else {
                            BOOST_LOG_TRIVIAL(info)
                                << "recv_object(): Got errno " << errno << " while recv data from " << sender_id;
                            std::lock_guard<std::mutex> lock(connections_lock);
                            connections.erase(sender_id);
                            ::close(fd);
                            break;
                        }
                    }
                }
            }

            if (need_sleep)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        BOOST_LOG_TRIVIAL(info) << "recv_object(): Recvd data from " << sender_id;
    }

    double DirectCheckpoint::get_latency(Utils::peer_num producer, Utils::peer_num consumer,
                                         std::size_t size_in_bytes) {
        return -1;
    }

    double DirectCheckpoint::get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) {
        return -1;
    }

    void DirectCheckpoint::teardown_fn() {
        shutdown.store(true);

        coordinator->delete_own_key(func_id);

        if (listener_thread.joinable())
            listener_thread.join();
        if (etcd_thread.joinable())
            etcd_thread.join();

        std::vector<int> peer_ids;
        for (const auto &[id, _] : connections)
            peer_ids.push_back(id);
        for (int id : peer_ids)
            drain_connection(id);
    }

    void DirectCheckpoint::restore_fn() {
        shutdown.store(false);

        listener_port = bind_and_listen();
        if (listener_port == -1) {
            BOOST_LOG_TRIVIAL(error) << "restore_fn(): Could not bind and listen to TCP port";
            throw std::runtime_error("Could not bind and listen to TCP port");
        }
        BOOST_LOG_TRIVIAL(info) << "restore_fn(): Peer " << func_id << " listens on TCP port " << listener_port;

        listener_thread = std::thread(&DirectCheckpoint::handle_listener, this);

        coordinator = std::make_unique<EtcdCoordinator>(etcd_host, etcd_port, comm_name);
        coordinator->advertise_own_key(func_id, listener_port);

        etcd_thread = std::thread(&DirectCheckpoint::handle_etcd, this);
    }

    void DirectCheckpoint::handle_listener() {
        struct pollfd pfd{listener_fd, POLLIN, 0};

        while (!shutdown.load()) {
            if (::poll(&pfd, 1, LISTENER_POLL_MS) <= 0)
                continue;

            struct sockaddr_in src{};
            socklen_t src_len = sizeof(src);
            int new_fd = ::accept(listener_fd, (struct sockaddr *)&src, &src_len);
            if (new_fd < 0)
                continue;

            BOOST_LOG_TRIVIAL(info) << "handle_listener(): New peer connected";
            struct timeval tv{0, RCVTIMEO_US};
            setsockopt(new_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

            int peer_func_id;
            if (::recv(new_fd, &peer_func_id, sizeof(peer_func_id), MSG_WAITALL) != sizeof(peer_func_id)) {
                BOOST_LOG_TRIVIAL(info) << "handle_listener(): Peer did not send identification successfully";
                ::close(new_fd);
                continue;
            }

            {
                BOOST_LOG_TRIVIAL(info) << "handle_listener(): Peer with id " << peer_func_id << " connected";
                std::lock_guard<std::mutex> lock(connections_lock);
                if (connections.find(peer_func_id) == connections.end())
                    connections[peer_func_id] = {new_fd, "", 0}; // listening port unknown for accepted connections
                else {
                    BOOST_LOG_TRIVIAL(error)
                        << "handle_listener(): Peer with id " << peer_func_id << " already connected";
                    ::close(new_fd);
                }
            }
        }

        ::close(listener_fd);
        listener_fd = -1;
        BOOST_LOG_TRIVIAL(info) << "handle_listener(): Thread stopped handling connections";
    }

    void DirectCheckpoint::handle_etcd() {
        // Read current state before watching so we don't miss peers that registered
        // while we were checkpointed.
        for (const auto &entry : coordinator->get_entries()) {
            if (entry.func_id != func_id && func_id < entry.func_id)
                connect_to(entry.func_id, entry.address, entry.port);
        }

        while (!shutdown.load()) {
            auto ev = coordinator->next_event(ETCD_POLL_MS);
            if (!ev)
                continue;

            const auto &entry = ev->entry;
            if (entry.func_id == func_id)
                continue;

            if (ev->type == EventType::PUT) {
                BOOST_LOG_TRIVIAL(info) << "handle_etcd(): PUT event for peer " << entry.func_id << ", address "
                                        << entry.address << ", port " << entry.port;

                // only connect to peers with higher id
                if (func_id < entry.func_id) {
                    bool known, same;
                    {
                        std::lock_guard<std::mutex> lock(connections_lock);
                        auto it = connections.find(entry.func_id);
                        known = (it != connections.end());
                        same = known && it->second.address == entry.address && it->second.port == entry.port;
                    }

                    if (!same) {
                        if (known) {
                            BOOST_LOG_TRIVIAL(info) << "handle_etcd(): PUT -> Drain connection to " << entry.func_id;
                            drain_connection(entry.func_id);
                        }

                        BOOST_LOG_TRIVIAL(info) << "handle_etcd(): PUT -> Connect to " << entry.func_id;
                        connect_to(entry.func_id, entry.address, entry.port);
                    }
                }
            } else { // DELETE
                BOOST_LOG_TRIVIAL(info) << "handle_etcd(): DELETE event for peer " << entry.func_id;
                drain_connection(entry.func_id);
            }
        }

        BOOST_LOG_TRIVIAL(info) << "handle_etcd(): Thread stopped handling etcd";
    }

    int DirectCheckpoint::bind_and_listen() {
        srand(time(nullptr));

        const int MAX_BIND_LISTEN_TRIES = 50;
        const int TCP_PORT_LB = 10000;
        const int TCP_PORT_RANGE = 10000;

        for (int i = 0; i < MAX_BIND_LISTEN_TRIES; i++) {
            int port = TCP_PORT_LB + rand() % TCP_PORT_RANGE;

            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (fd < 0) {
                BOOST_LOG_TRIVIAL(warning) << "bind_and_listen(): failed socket()";
                continue;
            }

            int one = 1;
            setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

            struct sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(port);

            if (::bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
                BOOST_LOG_TRIVIAL(warning) << "bind_and_listen(): failed bind()";
                ::close(fd);
                continue;
            }

            if (::listen(fd, SOMAXCONN) < 0) {
                BOOST_LOG_TRIVIAL(warning) << "bind_and_listen(): failed listen()";
                ::close(fd);
                continue;
            }

            listener_fd = fd;
            return port;
        }

        return -1;
    }

    void DirectCheckpoint::connect_to(int peer_id, const std::string &address, int port) {
        if (func_id >= peer_id) {
            BOOST_LOG_TRIVIAL(warning) << "connect_to(): Only connecting to peers with higher id";
            return;
        }

        {
            std::lock_guard<std::mutex> lock(connections_lock);
            if (connections.count(peer_id) > 0)
                return;
        }

        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            BOOST_LOG_TRIVIAL(warning) << "connect_to(): failed socket() for peer " << peer_id;
            return;
        }

        struct sockaddr_in dest{};
        dest.sin_family = AF_INET;
        dest.sin_port = htons(port);
        if (::inet_pton(AF_INET, address.c_str(), &dest.sin_addr) <= 0) {
            BOOST_LOG_TRIVIAL(warning) << "connect_to(): failed inet_pton() for peer " << peer_id;
            ::close(fd);
            return;
        }

        if (::connect(fd, (struct sockaddr *)&dest, sizeof(dest)) < 0) {
            BOOST_LOG_TRIVIAL(warning) << "connect_to(): failed connect() for peer " << peer_id;
            ::close(fd);
            return;
        }

        struct timeval tv{0, RCVTIMEO_US};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        if (::send(fd, &func_id, sizeof(func_id), 0) != sizeof(func_id)) {
            BOOST_LOG_TRIVIAL(warning) << "connect_to(): failed send() for peer " << peer_id;
            ::close(fd);
            return;
        }

        std::lock_guard<std::mutex> lock(connections_lock);
        if (connections.count(peer_id) > 0) {
            BOOST_LOG_TRIVIAL(error) << "connect_to(): Peer with id " << peer_id << " already connected";
            ::close(fd);
            return;
        }
        connections[peer_id] = {fd, address, port};
    }

    void DirectCheckpoint::drain_connection(int peer_id) {
        int fd;
        {
            std::lock_guard<std::mutex> lock(connections_lock);
            auto it = connections.find(peer_id);
            if (it == connections.end())
                return;
            fd = it->second.fd;
            connections.erase(it);
        }

        ::shutdown(fd, SHUT_WR);

        struct timeval tv{0, DRAIN_RCVTIMEO_US};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        char tmp[4096];
        while (true) {
            ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
            if (n <= 0)
                break;
            std::lock_guard<std::mutex> lock(recv_buffers_lock);
            auto &q = recv_buffers[peer_id];
            q.insert(q.end(), tmp, tmp + n);

            BOOST_LOG_TRIVIAL(info) << "drain_connection(): Received " << n << " bytes from " << peer_id;
        }

        ::close(fd);
    }

} // namespace FMI::Comm
