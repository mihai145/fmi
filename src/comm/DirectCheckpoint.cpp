#include "../../include/comm/DirectCheckpoint.h"

#include "utils.hpp"

#include <arpa/inet.h>
#include <boost/log/trivial.hpp>
#include <cerrno>
#include <chrono>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_set>

namespace {
    constexpr int RCVTIMEO_US = 5 * 1000;
    constexpr int DRAIN_BUDGET_MS = 300;
    constexpr int DRAIN_RCVTIMEO_US = DRAIN_BUDGET_MS * 1000;
    constexpr int DRAIN_SAFETY_MS = 5000;
    constexpr int LISTENER_POLL_MS = 15;
    constexpr int ETCD_POLL_MS = 200;
    constexpr int MISSING_CONN_SLEEP_MS = 1;
    constexpr int POLL_TIMEOUT_MS = 100;
    constexpr int ADVERTISE_RETRIES = 10;
    constexpr int CONNECT_TO_FAILURE_BACKOFF_MS = 100;
    constexpr int SELF_STOP_TIMEOUT_MS = 3000;
} // namespace

namespace FMI::Comm {

    DirectCheckpoint::DirectCheckpoint(std::map<std::string, std::string> params,
                                       std::map<std::string, std::string> model_params) {
        etcd_host = params["etcd_host"];
        etcd_port = std::stoi(params["etcd_port"]);
        func_id = std::stoi(getenv("func_id"));

        checkpoint::Checkpointer::setup_thread();
        restore_fn();

        state.channel_type = 2; // p2p
        state.num_peers = std::stoi(getenv("num_peers"));

        checkpointer = std::make_unique<checkpoint::Checkpointer>([this] {
            checkpointer->register_hint(state);
            teardown_fn();
        }, [this] { restore_fn(); });
    }

    DirectCheckpoint::~DirectCheckpoint() {
        if (checkpointer)
            checkpointer->shutdown_ctrl_thread();

        shutdown.store(true);
        if (listener_thread.joinable())
            listener_thread.join();
        if (etcd_thread.joinable())
            etcd_thread.join();

        // remove our own key
        if (coordinator) {
            try {
                coordinator->delete_own_key(func_id);
            } catch (const std::exception &e) {
                BOOST_LOG_TRIVIAL(warning) << "~DirectCheckpoint(): delete_own_key failed: " << e.what();
            }
        }

        if (checkpointer)
            checkpointer->register_hint(state);
    }

    void DirectCheckpoint::send_object(channel_data buf, Utils::peer_num rcpt_id) {
        int sent = 0;

        while (sent < (int)buf.len) {
            int fd = -1;
            bool blocked = false;
            int cnt_restore = 0;
            {
                auto ctx = checkpointer->get_uninterruptible_context();
                cnt_restore = checkpointer->get_cnt_restore();
                hint_send_wait(rcpt_id);

                {
                    std::lock_guard<std::mutex> lock(connections_lock);
                    auto it = connections.find(rcpt_id);
                    if (it != connections.end())
                        fd = it->second.fd;
                }

                if (fd != -1) {
                    ssize_t n = ::send(fd, buf.buf + sent, buf.len - sent, MSG_NOSIGNAL | MSG_DONTWAIT);
                    if (n > 0) {
                        stall_reset();
                        sent += n;
                        if (sent == (int)buf.len) hint_send_done(rcpt_id);
                        continue;
                    } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        blocked = true;
                    } else {
                        BOOST_LOG_TRIVIAL(info)
                            << "send_object(): Got errno " << errno << " while sending data to " << rcpt_id;
                        std::lock_guard<std::mutex> lock(connections_lock);
                        auto it = connections.find(rcpt_id);
                        if (it != connections.end() && it->second.fd == fd) {
                            connections.erase(it);
                            ::close(fd);
                        }
                        fd = -1;
                    }
                }
            }

            if (fd == -1) {
                stall_check(rcpt_id, cnt_restore);
                std::this_thread::sleep_for(std::chrono::milliseconds(MISSING_CONN_SLEEP_MS));
            } else if (blocked) {
                stall_reset();
                struct pollfd pfd{fd, POLLOUT, 0};
                ::poll(&pfd, 1, POLL_TIMEOUT_MS);
            }
        }

        // BOOST_LOG_TRIVIAL(info) << "send_object(): Sent data to " << rcpt_id;
    }

    void DirectCheckpoint::recv_object(channel_data buf, Utils::peer_num sender_id) {
        int recvd = 0;

        while (recvd < (int)buf.len) {
            int fd = -1;
            bool blocked = false;
            int cnt_restore = 0;
            {
                auto ctx = checkpointer->get_uninterruptible_context();
                cnt_restore = checkpointer->get_cnt_restore();
                hint_recv_wait(sender_id);

                {
                    std::lock_guard<std::mutex> lock(recv_buffers_lock);
                    auto it = recv_buffers.find(sender_id);
                    if (it != recv_buffers.end() && !it->second.empty()) {
                        auto &q = it->second;
                        int to_copy = std::min((int)q.size(), (int)buf.len - recvd);
                        std::copy(q.begin(), q.begin() + to_copy, buf.buf + recvd);
                        q.erase(q.begin(), q.begin() + to_copy);
                        recvd += to_copy;
                        stall_reset();
                        BOOST_LOG_TRIVIAL(info) << "recv_object(): Drained " << to_copy << " bytes from " << sender_id;

                        if (recvd == (int)buf.len) hint_recv_done(sender_id);
                        continue;
                    }
                }

                {
                    std::lock_guard<std::mutex> lock(connections_lock);
                    auto it = connections.find(sender_id);
                    if (it != connections.end())
                        fd = it->second.fd;
                }

                if (fd != -1) {
                    ssize_t n = ::recv(fd, buf.buf + recvd, buf.len - recvd, MSG_DONTWAIT);
                    if (n > 0) {
                        stall_reset();
                        recvd += n;
                        if (recvd == (int)buf.len) hint_recv_done(sender_id);
                        continue;
                    } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        blocked = true;
                    } else {
                        BOOST_LOG_TRIVIAL(info) << "recv_object(): connection to " << sender_id
                                                << " closed/error (n=" << n << ", errno=" << errno << ")";
                        std::lock_guard<std::mutex> lock(connections_lock);
                        auto it = connections.find(sender_id);
                        if (it != connections.end() && it->second.fd == fd) {
                            connections.erase(it);
                            ::close(fd);
                        }
                        fd = -1;
                    }
                }
            }

            if (fd == -1) {
                stall_check(sender_id, cnt_restore);
                std::this_thread::sleep_for(std::chrono::milliseconds(MISSING_CONN_SLEEP_MS));
            } else if (blocked) {
                stall_reset();
                struct pollfd pfd{fd, POLLIN, 0};
                ::poll(&pfd, 1, POLL_TIMEOUT_MS);
            }
        }

        // BOOST_LOG_TRIVIAL(info) << "recv_object(): Recvd data from " << sender_id;
    }

    void DirectCheckpoint::stall_check(int peer, int cnt_restore) {
        if (presence.live(peer)) {  // disarm the tracker if the peer is alive
            stall_reset();
            return;
        }

        // rearm the tracker is it's not armed, the peer has changed or we straddled a checkpoint
        auto now = std::chrono::steady_clock::now();
        if (!stall.armed || stall.peer != peer || stall.cnt_restore != cnt_restore) {
            stall = {cnt_restore, peer, now, true, false};
            return;
        }

        if (stall.triggered)
            return;

        if (now - stall.since >= std::chrono::milliseconds(SELF_STOP_TIMEOUT_MS)) {
            BOOST_LOG_TRIVIAL(warning) << "stall_check(): peer " << peer << " not live for " << SELF_STOP_TIMEOUT_MS
                                       << "ms (cnt_restore " << cnt_restore << "), triggering self stop";
            stall.triggered = true;
            checkpointer->self_stop(cnt_restore);
        }
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

        auto metrics =
            common::func_metrics(std::to_string(checkpointer->get_job_id()),
                                 std::to_string(checkpointer->get_func_id()), checkpointer->get_cnt_restore());

        {
            auto m = metrics.start("teardown_delete_key");
            try {
                coordinator->delete_own_key(func_id);
            } catch (const std::exception &e) {
                BOOST_LOG_TRIVIAL(warning) << "teardown_fn(): delete_own_key failed: " << e.what();
            }
            m.stop();
        }

        {
            auto m = metrics.start("teardown_join_listener");
            if (listener_thread.joinable())
                listener_thread.join();
            m.stop();
        }

        {
            auto m = metrics.start("teardown_join_etcd");
            if (etcd_thread.joinable())
                etcd_thread.join();
            m.stop();
        }

        {
            // Close the persistent /v3/watch connection
            auto m = metrics.start("teardown_stop_watch");
            if (coordinator)
                coordinator->stop_watch();
            m.stop();
        }

        {
            auto m = metrics.start("teardown_drain");
            std::vector<int> peer_ids;
            for (const auto &[id, _] : connections)
                peer_ids.push_back(id);
            drain_all(peer_ids);
            m.stop();
        }
    }

    void DirectCheckpoint::restore_fn() {
        shutdown.store(false);
        presence.reset(); // repopulated from get_entries

        listener_port = bind_and_listen();
        if (listener_port == -1) {
            BOOST_LOG_TRIVIAL(error) << "restore_fn(): Could not bind and listen to TCP port";
            throw std::runtime_error("Could not bind and listen to TCP port");
        }
        BOOST_LOG_TRIVIAL(info) << "restore_fn(): Peer " << func_id << " listens on TCP port " << listener_port;

        listener_thread = std::thread(&DirectCheckpoint::handle_listener, this);

        coordinator = std::make_unique<EtcdCoordinator>(etcd_host, etcd_port, getenv("job_id"));
        int cnt_restore = (checkpointer == nullptr) ? 0 : checkpointer->get_cnt_restore();
        for (int attempt = 1;; attempt++) {
            try {
                coordinator->advertise_conn(func_id, cnt_restore, listener_port);
                break;
            } catch (const std::exception &e) {
                if (attempt == ADVERTISE_RETRIES)
                    throw;
                BOOST_LOG_TRIVIAL(warning)
                    << "restore_fn(): advertise_conn failed (attempt " << attempt << "): " << e.what();
                std::this_thread::sleep_for(std::chrono::milliseconds(ETCD_POLL_MS));
            }
        }

        etcd_thread = std::thread(&DirectCheckpoint::handle_etcd, this);
    }

    void DirectCheckpoint::handle_listener() {
        int epfd = ::epoll_create1(0);
        if (epfd < 0) {
            BOOST_LOG_TRIVIAL(error) << "handle_listener(): epoll_create1 failed";
            return;
        }

        struct epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = listener_fd;
        ::epoll_ctl(epfd, EPOLL_CTL_ADD, listener_fd, &ev);

        // fds accepted but not yet identified (handshake recv pending)
        std::unordered_set<int> pending;

        struct epoll_event events[16];

        while (!shutdown.load()) {
            int n = ::epoll_wait(epfd, events, 16, LISTENER_POLL_MS);
            if (n <= 0)
                continue;

            for (int i = 0; i < n; i++) {
                int fd = events[i].data.fd;

                if (fd == listener_fd) {
                    struct sockaddr_in src{};
                    socklen_t src_len = sizeof(src);
                    int new_fd = ::accept(listener_fd, (struct sockaddr *)&src, &src_len);
                    if (new_fd < 0)
                        continue;

                    struct timeval tv{0, RCVTIMEO_US};
                    setsockopt(new_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

                    struct epoll_event nev{};
                    nev.events = EPOLLIN;
                    nev.data.fd = new_fd;
                    ::epoll_ctl(epfd, EPOLL_CTL_ADD, new_fd, &nev);
                    pending.insert(new_fd);

                    BOOST_LOG_TRIVIAL(info) << "handle_listener(): New peer connected";
                } else {
                    // handshake ready: remove from epoll before recv
                    ::epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                    pending.erase(fd);

                    int peer_func_id;
                    if (::recv(fd, &peer_func_id, sizeof(peer_func_id), MSG_WAITALL) != sizeof(peer_func_id)) {
                        BOOST_LOG_TRIVIAL(info) << "handle_listener(): Peer did not send identification successfully";
                        ::close(fd);
                        continue;
                    }

                    BOOST_LOG_TRIVIAL(info) << "handle_listener(): Peer with id " << peer_func_id << " connected";
                    std::lock_guard<std::mutex> lock(connections_lock);
                    if (connections.find(peer_func_id) == connections.end())
                        connections[peer_func_id] = {fd, "", 0};
                    else {
                        BOOST_LOG_TRIVIAL(error)
                            << "handle_listener(): Peer with id " << peer_func_id << " already connected";
                        ::close(fd);
                    }
                }
            }
        }

        for (int fd : pending)
            ::close(fd);
        ::close(epfd);
        ::close(listener_fd);
        listener_fd = -1;
        BOOST_LOG_TRIVIAL(info) << "handle_listener(): Thread stopped handling connections";
    }

    void DirectCheckpoint::handle_etcd() {
        pending_connects.clear();

        // Read current state before watching so we don't miss peers that registered
        // while we were checkpointed.
        std::vector<Entry> entries;
        {
            auto metrics = common::func_metrics(getenv("job_id"), std::to_string(func_id),
                                                (checkpointer == nullptr) ? 0 : checkpointer->get_cnt_restore());
            auto m = metrics.start("etcd_get_entries");
            for (int attempt = 1;; attempt++) {
                try {
                    entries = coordinator->get_entries();
                    break;
                } catch (const std::exception &e) {
                    if (attempt == ADVERTISE_RETRIES) {
                        BOOST_LOG_TRIVIAL(error) << "handle_etcd(): get_entries failed after retries: " << e.what();
                        m.stop();
                        return;
                    }
                    BOOST_LOG_TRIVIAL(warning)
                        << "handle_etcd(): get_entries failed (attempt " << attempt << "): " << e.what();
                    std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_TO_FAILURE_BACKOFF_MS));
                }
            }
            m.stop();
        }

        for (const auto &entry : entries) {
            if (entry.func_id == func_id)
                continue;
            process({entry.reachable() ? EventType::ADVERTISE_CONN : EventType::ADVERTISE_START, entry});
        }

        coordinator->start_watch();

        while (!shutdown.load()) {
            // next_event() blocks until an event or the timeout
            // wake often enough to retry pending connects
            int timeout = pending_connects.empty() ? ETCD_POLL_MS : CONNECT_TO_FAILURE_BACKOFF_MS;
            auto ev = coordinator->next_event(timeout);

            // handle retries first
            auto now = std::chrono::steady_clock::now();
            for (auto it = pending_connects.begin(); it != pending_connects.end();) {
                if (now >= it->second.next_retry) {
                    if (connect_to(it->first, it->second.address, it->second.port)) {
                        it = pending_connects.erase(it);
                    } else {
                        it->second.next_retry = now + std::chrono::milliseconds(CONNECT_TO_FAILURE_BACKOFF_MS);
                        ++it;
                    }
                } else {
                    ++it;
                }
            }

            if (!ev)
                continue;

            if (ev->entry.func_id == func_id)
                continue;

            process(*ev);
        }

        BOOST_LOG_TRIVIAL(info) << "handle_etcd(): Thread stopped handling etcd";
    }

    void DirectCheckpoint::advertised_start(const Entry &entry) {
        BOOST_LOG_TRIVIAL(info) << "advertised_start(): peer " << entry.func_id
                                << " advertised to start (cnt_restore " << entry.cnt_restore << ")";
        presence.set(entry.func_id, true);
    }

    void DirectCheckpoint::advertised_conn(const Entry &entry) {
        BOOST_LOG_TRIVIAL(info) << "advertised_conn(): peer " << entry.func_id << " reachable at " << *entry.address
                                << ":" << *entry.port << " (cnt_restore " << entry.cnt_restore << ")";

        presence.set(entry.func_id, true);
        pending_connects.erase(entry.func_id);

        // only connect to peers with higher id
        if (func_id >= entry.func_id)
            return;

        bool known, same;
        {
            std::lock_guard<std::mutex> lock(connections_lock);
            auto it = connections.find(entry.func_id);
            known = (it != connections.end());
            same = known && it->second.address == *entry.address && it->second.port == *entry.port;
        }

        if (same)
            return;

        if (known) {
            BOOST_LOG_TRIVIAL(info) << "advertised_conn(): Drain connection to " << entry.func_id;
            drain_connection(entry.func_id);
        }

        BOOST_LOG_TRIVIAL(info) << "advertised_conn(): Connect to " << entry.func_id;
        if (!connect_to(entry.func_id, *entry.address, *entry.port))
            pending_connects[entry.func_id] = {*entry.address, *entry.port,
                                               std::chrono::steady_clock::now() +
                                                   std::chrono::milliseconds(CONNECT_TO_FAILURE_BACKOFF_MS)};
    }

    void DirectCheckpoint::advertised_leave(const Entry &entry) {
        BOOST_LOG_TRIVIAL(info) << "advertised_leave(): peer " << entry.func_id << " left";
        presence.set(entry.func_id, false);
        pending_connects.erase(entry.func_id);
        drain_connection(entry.func_id);
    }

    int DirectCheckpoint::bind_and_listen() {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            BOOST_LOG_TRIVIAL(warning) << "bind_and_listen(): failed socket()";
            return -1;
        }

        int one = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(0); // let the OS pick a free port

        if (::bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            BOOST_LOG_TRIVIAL(warning) << "bind_and_listen(): failed bind()";
            ::close(fd);
            return -1;
        }

        if (::listen(fd, SOMAXCONN) < 0) {
            BOOST_LOG_TRIVIAL(warning) << "bind_and_listen(): failed listen()";
            ::close(fd);
            return -1;
        }

        socklen_t len = sizeof(addr);
        if (::getsockname(fd, (struct sockaddr *)&addr, &len) < 0) {
            BOOST_LOG_TRIVIAL(warning) << "bind_and_listen(): failed getsockname()";
            ::close(fd);
            return -1;
        }

        listener_fd = fd;
        return ntohs(addr.sin_port);
    }

    bool DirectCheckpoint::connect_to(int peer_id, const std::string &address, int port) {
        if (func_id >= peer_id) {
            BOOST_LOG_TRIVIAL(warning) << "connect_to(): Only connecting to peers with higher id";
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(connections_lock);
            if (connections.count(peer_id) > 0)
                return true;
        }

        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            BOOST_LOG_TRIVIAL(warning) << "connect_to(): failed socket() for peer " << peer_id;
            return false;
        }

        struct sockaddr_in dest{};
        dest.sin_family = AF_INET;
        dest.sin_port = htons(port);
        if (::inet_pton(AF_INET, address.c_str(), &dest.sin_addr) <= 0) {
            BOOST_LOG_TRIVIAL(warning) << "connect_to(): failed inet_pton() for peer " << peer_id;
            ::close(fd);
            return false;
        }

        if (::connect(fd, (struct sockaddr *)&dest, sizeof(dest)) < 0) {
            BOOST_LOG_TRIVIAL(warning) << "connect_to(): failed connect() for peer " << peer_id;
            ::close(fd);
            return false;
        }

        struct timeval tv{0, RCVTIMEO_US};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        if (::send(fd, &func_id, sizeof(func_id), 0) != sizeof(func_id)) {
            BOOST_LOG_TRIVIAL(warning) << "connect_to(): failed send() for peer " << peer_id;
            ::close(fd);
            return false;
        }

        std::lock_guard<std::mutex> lock(connections_lock);
        if (connections.count(peer_id) > 0) {
            BOOST_LOG_TRIVIAL(error) << "connect_to(): Peer with id " << peer_id << " already connected";
            ::close(fd);
            return true;
        }
        connections[peer_id] = {fd, address, port};
        return true;
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

    void DirectCheckpoint::drain_all(const std::vector<int> &peer_ids) {
        struct Draining {
            int fd;
            int peer_id;
            bool open;
        };

        std::vector<Draining> draining;
        {
            std::lock_guard<std::mutex> lock(connections_lock);
            for (int peer_id : peer_ids) {
                auto it = connections.find(peer_id);
                if (it == connections.end())
                    continue;
                draining.push_back({it->second.fd, peer_id, true});
                connections.erase(it);
            }
        }

        for (const auto &d : draining)
            ::shutdown(d.fd, SHUT_WR);

        auto metrics =
            common::func_metrics(std::to_string(checkpointer->get_job_id()),
                                 std::to_string(checkpointer->get_func_id()), checkpointer->get_cnt_restore());
        auto m_drain = metrics.start("drain_all");

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(DRAIN_SAFETY_MS);
        char tmp[4096];
        size_t remaining = draining.size();

        while (remaining > 0) {
            int timeout =
                (int)std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now())
                    .count();
            if (timeout <= 0)
                break; // safety deadline hit

            std::vector<struct pollfd> pfds;
            std::vector<Draining *> slots;
            pfds.reserve(remaining);
            slots.reserve(remaining);
            for (auto &d : draining) {
                if (!d.open)
                    continue;
                pfds.push_back({d.fd, POLLIN, 0});
                slots.push_back(&d);
            }

            int r = ::poll(pfds.data(), pfds.size(), timeout);
            if (r < 0) {
                if (errno == EINTR)
                    continue;
                BOOST_LOG_TRIVIAL(warning) << "drain_all(): poll failed: errno " << errno;
                break;
            }
            if (r == 0)
                break;

            for (size_t i = 0; i < pfds.size(); i++) {
                if (!(pfds[i].revents & (POLLIN | POLLHUP | POLLERR)))
                    continue;

                Draining *d = slots[i];
                while (true) {
                    ssize_t n = ::recv(d->fd, tmp, sizeof(tmp), MSG_DONTWAIT);
                    if (n > 0) {
                        std::lock_guard<std::mutex> lock(recv_buffers_lock);
                        auto &q = recv_buffers[d->peer_id];
                        q.insert(q.end(), tmp, tmp + n);
                    } else if (n == 0) {
                        d->open = false; // clean EOF: fully drained
                        break;
                    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break; // nothing available right now: keep the fd and poll again
                    } else {
                        BOOST_LOG_TRIVIAL(warning) << "drain_all(): recv error from " << d->peer_id << ": errno "
                                                   << errno << " -- closing before EOF, bytes may be lost";
                        d->open = false;
                        break;
                    }
                }

                if (!d->open) {
                    ::close(d->fd);
                    remaining--;
                }
            }
        }

        if (remaining > 0)
            BOOST_LOG_TRIVIAL(warning) << "drain_all(): " << remaining << " of " << draining.size()
                                       << " peers did not reach EOF within " << DRAIN_SAFETY_MS
                                       << "ms; force-closing -- in-flight bytes may be lost";

        for (const auto &d : draining)
            if (d.open)
                ::close(d.fd);

        m_drain.extra("peers", std::to_string(draining.size()));
        m_drain.extra("forced", std::to_string(remaining));
        m_drain.stop();
    }

} // namespace FMI::Comm
