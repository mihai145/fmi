#include "../../include/comm/EtcdCoordinator.h"
#include "utils.hpp"

#include <boost/log/trivial.hpp>
#include <chrono>
#include <curl/curl.h>
#include <mutex>
#include <openssl/evp.h>
#include <stdexcept>
#include <thread>

namespace {

    constexpr int CURL_TIMEOUT_MS = 300;
    constexpr int WATCH_POLL_TIMEOUT_MS = 1000;     // safety net, wakeup handles fast shutdown
    constexpr int WATCH_RECONNECT_BACKOFF_MS = 200; // pause before re-opening a dropped stream

    std::string b64_encode(const std::string &in) {
        std::string out(4 * ((in.size() + 2) / 3), '\0');
        EVP_EncodeBlock((unsigned char *)out.data(), (const unsigned char *)in.data(), in.size());
        return out;
    }

    std::string b64_decode(const std::string &in) {
        std::string out(in.size() * 3 / 4, '\0');
        int len = EVP_DecodeBlock((unsigned char *)out.data(), (const unsigned char *)in.data(), in.size());
        if (len < 0)
            return "";
        out.resize(len - std::count(in.begin(), in.end(), '='));
        return out;
    }

    size_t write_cb(char *ptr, size_t size, size_t nmemb, void *ud) {
        static_cast<std::string *>(ud)->append(ptr, size * nmemb);
        return size * nmemb;
    }

    std::string http_post(const std::string &url, const std::string &body, int timeout_ms) {
        CURL *curl = curl_easy_init();
        if (!curl)
            throw std::runtime_error("curl_easy_init failed");

        std::string resp;
        struct curl_slist *hdrs = nullptr;
        hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
        hdrs = curl_slist_append(hdrs, "Expect:");

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms);

        CURLcode rc = curl_easy_perform(curl);
        curl_slist_free_all(hdrs);
        curl_easy_cleanup(curl);

        if (rc != CURLE_OK)
            throw std::runtime_error(std::string("curl: ") + curl_easy_strerror(rc));
        return resp;
    }

    // Extract all {key, value} pairs from an etcd /v3/kv/range JSON response.
    std::vector<std::pair<std::string, std::string>> parse_kvs(const std::string &json) {
        std::vector<std::pair<std::string, std::string>> result;
        size_t pos = 0;
        while (true) {
            auto kp = json.find("\"key\":\"", pos);
            if (kp == std::string::npos)
                break;
            kp += 7;
            auto ke = json.find('"', kp);

            auto vp = json.find("\"value\":\"", ke);
            if (vp == std::string::npos)
                break;
            vp += 9;
            auto ve = json.find('"', vp);

            result.push_back({b64_decode(json.substr(kp, ke - kp)), b64_decode(json.substr(vp, ve - vp))});
            pos = ve;
        }
        return result;
    }

    // value: "<cnt_restore>" or "<cnt_restore>:<ip>:<port>"
    std::optional<FMI::Comm::Entry> parse_value(int func_id, const std::string &raw_val) {
        try {
            FMI::Comm::Entry e{func_id, 0, std::nullopt, std::nullopt};
            auto c1 = raw_val.find(':');
            if (c1 == std::string::npos) {
                e.cnt_restore = std::stoi(raw_val);
                return e;
            }
            auto c2 = raw_val.rfind(':');
            if (c2 == c1)
                return std::nullopt;
            e.cnt_restore = std::stoi(raw_val.substr(0, c1));
            e.address = raw_val.substr(c1 + 1, c2 - c1 - 1);
            e.port = std::stoi(raw_val.substr(c2 + 1));
            return e;
        } catch (const std::exception &) {
            return std::nullopt;
        }
    }

    long parse_header_revision(const std::string &json) {
        auto rp = json.find("\"revision\":\"");
        if (rp == std::string::npos)
            return -1;
        rp += 12;
        auto re = json.find('"', rp);
        if (re == std::string::npos)
            return -1;
        return std::stol(json.substr(rp, re - rp));
    }

} // namespace

namespace FMI::Comm {

    EtcdCoordinator::EtcdCoordinator(const std::string &etcd_host, int etcd_port, const std::string &job_id)
        : base_url("http://" + etcd_host + ":" + std::to_string(etcd_port)), key_prefix(job_id + "/") {
        static std::once_flag init_flag;
        std::call_once(init_flag, [] { curl_global_init(CURL_GLOBAL_ALL); });
    }

    EtcdCoordinator::~EtcdCoordinator() { stop_watch(); }

    void EtcdCoordinator::advertise_conn(int func_id, int cnt_restore, int port) {
        std::string key = key_prefix + std::to_string(func_id);
        std::string value =
            std::to_string(cnt_restore) + ":" + common::get_rdma_ip() + ":" + std::to_string(port);
        std::string body = R"({"key":")" + b64_encode(key) + R"(","value":")" + b64_encode(value) + "\"}";
        http_post(base_url + "/v3/kv/put", body, CURL_TIMEOUT_MS);
        BOOST_LOG_TRIVIAL(info) << "EtcdCoordinator: advertised " << key << " = " << value;
    }

    void EtcdCoordinator::delete_own_key(int func_id) {
        std::string key = key_prefix + std::to_string(func_id);
        std::string body = R"({"key":")" + b64_encode(key) + "\"}";
        http_post(base_url + "/v3/kv/deleterange", body, CURL_TIMEOUT_MS);
        BOOST_LOG_TRIVIAL(info) << "EtcdCoordinator: deleted " << key;
    }

    std::vector<Entry> EtcdCoordinator::fetch_range(int timeout_ms) {
        std::string pfx_end = key_prefix;
        pfx_end.back()++; // prefix-range
        std::string body = R"({"key":")" + b64_encode(key_prefix) + R"(","range_end":")" + b64_encode(pfx_end) + "\"}";
        std::string resp = http_post(base_url + "/v3/kv/range", body, timeout_ms);

        // store revision so the watch can resume exactly after this read
        long rev = parse_header_revision(resp);
        if (rev >= 0)
            last_revision = rev;

        std::vector<Entry> entries;
        for (auto &[raw_key, raw_val] : parse_kvs(resp)) {
            // key: "job_id/func_id"
            auto slash = raw_key.rfind('/');
            if (slash == std::string::npos)
                continue;
            auto entry = parse_value(std::stoi(raw_key.substr(slash + 1)), raw_val);
            if (entry)
                entries.push_back(*entry);
        }
        return entries;
    }

    std::vector<Entry> EtcdCoordinator::get_entries() { return fetch_range(CURL_TIMEOUT_MS); }

    std::optional<WatchEvent> EtcdCoordinator::next_event(int timeout_ms) {
        std::unique_lock<std::mutex> lock(queue_mutex);
        queue_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return !event_queue.empty(); });
        if (event_queue.empty())
            return std::nullopt;
        auto ev = event_queue.front();
        event_queue.pop_front();
        return ev;
    }

    void EtcdCoordinator::start_watch() {
        if (watch_thread.joinable())
            return;
        watch_stop.store(false);
        watch_thread = std::thread(&EtcdCoordinator::watch_loop, this);
    }

    void EtcdCoordinator::stop_watch() {
        watch_stop.store(true);
        {
            std::lock_guard<std::mutex> lock(watch_multi_mutex);
            if (watch_multi)
                curl_multi_wakeup(static_cast<CURLM *>(watch_multi));
        }
        if (watch_thread.joinable())
            watch_thread.join();
    }

    size_t EtcdCoordinator::watch_write_cb(char *ptr, size_t size, size_t nmemb, void *ud) {
        size_t n = size * nmemb;
        static_cast<EtcdCoordinator *>(ud)->on_watch_data(ptr, n);
        return n;
    }

    // The gRPC-gateway streams one JSON frame per line: {"result":{"header":{...},"events":[...]}}.
    // Buffer partial lines and parse each complete one.
    void EtcdCoordinator::on_watch_data(const char *data, size_t len) {
        watch_buf.append(data, len);

        size_t nl;
        while ((nl = watch_buf.find('\n')) != std::string::npos) {
            std::string line = watch_buf.substr(0, nl);
            watch_buf.erase(0, nl + 1);

            long rev = parse_header_revision(line);
            if (rev > last_revision)
                last_revision = rev;

            std::vector<WatchEvent> events;
            try {
                events = parse_watch_events(line);
            } catch (const std::exception &e) {
                BOOST_LOG_TRIVIAL(warning) << "EtcdCoordinator: failed to parse watch frame: " << e.what();
                continue;
            }
            if (events.empty())
                continue;

            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                for (auto &e : events)
                    event_queue.push_back(e);
            }
            queue_cv.notify_one();
        }
    }

    // Turn one watch frame's events[] into WatchEvents
    std::vector<WatchEvent> EtcdCoordinator::parse_watch_events(const std::string &line) {
        std::vector<WatchEvent> out;

        size_t pos = 0;
        while (true) {
            auto kp = line.find("\"key\":\"", pos);
            if (kp == std::string::npos)
                break;
            kp += 7;
            auto ke = line.find('"', kp);
            if (ke == std::string::npos)
                break;
            std::string raw_key = b64_decode(line.substr(kp, ke - kp));

            auto next_key = line.find("\"key\":\"", ke);
            size_t window_end = (next_key == std::string::npos) ? line.size() : next_key;
            pos = window_end;

            auto slash = raw_key.rfind('/');
            if (slash == std::string::npos)
                continue;
            int fid = std::stoi(raw_key.substr(slash + 1));

            auto vp = line.find("\"value\":\"", ke);
            if (vp != std::string::npos && vp < window_end) {
                vp += 9;
                auto ve = line.find('"', vp);
                if (ve == std::string::npos)
                    break;
                std::string raw_val = b64_decode(line.substr(vp, ve - vp));
                auto entry = parse_value(fid, raw_val);
                if (!entry)
                    continue;
                out.push_back({entry->reachable() ? EventType::ADVERTISE_CONN : EventType::ADVERTISE_START, *entry});
            } else {
                out.push_back({EventType::ADVERTISE_LEAVE, {fid, -1, std::nullopt, std::nullopt}});
            }
        }
        return out;
    }

    // One long-lived POST /v3/watch per connection, re-opened if the stream drops. Driven by the
    // curl multi interface so stop_watch() can interrupt curl_multi_poll() via curl_multi_wakeup().
    void EtcdCoordinator::watch_loop() {
        std::string pfx_end = key_prefix;
        pfx_end.back()++;

        while (!watch_stop.load()) {
            std::string body = R"({"create_request":{"key":")" + b64_encode(key_prefix) + R"(","range_end":")" +
                               b64_encode(pfx_end) + R"(","start_revision":")" + std::to_string(last_revision + 1) +
                               "\"}}";

            CURL *easy = curl_easy_init();
            CURLM *multi = curl_multi_init();
            if (!easy || !multi) {
                if (easy)
                    curl_easy_cleanup(easy);
                if (multi)
                    curl_multi_cleanup(multi);
                BOOST_LOG_TRIVIAL(error) << "EtcdCoordinator: failed to init watch curl handles";
                break;
            }

            struct curl_slist *hdrs = nullptr;
            hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
            hdrs = curl_slist_append(hdrs, "Expect:");

            std::string url = base_url + "/v3/watch";
            curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
            curl_easy_setopt(easy, CURLOPT_POSTFIELDS, body.c_str());
            curl_easy_setopt(easy, CURLOPT_HTTPHEADER, hdrs);
            curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, watch_write_cb);
            curl_easy_setopt(easy, CURLOPT_WRITEDATA, this);

            watch_buf.clear();
            curl_multi_add_handle(multi, easy);
            {
                std::lock_guard<std::mutex> lock(watch_multi_mutex);
                watch_multi = multi;
            }

            int running = 0;
            do {
                CURLMcode mc = curl_multi_perform(multi, &running);
                if (mc == CURLM_OK && running) {
                    int numfds = 0;
                    mc = curl_multi_poll(multi, nullptr, 0, WATCH_POLL_TIMEOUT_MS, &numfds);
                }
                if (mc != CURLM_OK)
                    break;
            } while (running && !watch_stop.load());

            // Hold the lock across teardown so a concurrent stop_watch() never wakes a freed handle.
            {
                std::lock_guard<std::mutex> lock(watch_multi_mutex);
                watch_multi = nullptr;
                curl_multi_remove_handle(multi, easy);
                curl_easy_cleanup(easy);
                curl_multi_cleanup(multi);
            }
            curl_slist_free_all(hdrs);

            if (watch_stop.load())
                break;

            // reconnect
            BOOST_LOG_TRIVIAL(warning) << "EtcdCoordinator: watch stream ended, reconnecting";
            std::this_thread::sleep_for(std::chrono::milliseconds(WATCH_RECONNECT_BACKOFF_MS));
        }

        BOOST_LOG_TRIVIAL(info) << "EtcdCoordinator: watch thread stopped";
    }

} // namespace FMI::Comm
