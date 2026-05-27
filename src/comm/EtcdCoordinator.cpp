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

    constexpr int CURL_TIMEOUT_MS = 1000;

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

} // namespace

namespace FMI::Comm {

    EtcdCoordinator::EtcdCoordinator(const std::string &etcd_host, int etcd_port, const std::string &comm_name)
        : base_url("http://" + etcd_host + ":" + std::to_string(etcd_port)), key_prefix(comm_name + "/") {
        static std::once_flag init_flag;
        std::call_once(init_flag, [] { curl_global_init(CURL_GLOBAL_ALL); });
    }

    void EtcdCoordinator::advertise_own_key(int func_id, int port) {
        std::string key = key_prefix + std::to_string(func_id);
        std::string value = common::get_ethernet_ip() + ":" + std::to_string(port);
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

        std::vector<Entry> entries;
        for (auto &[raw_key, raw_val] : parse_kvs(resp)) {
            // key: "comm_name/func_id", value: "ip:port"
            auto slash = raw_key.rfind('/');
            auto colon = raw_val.rfind(':');
            if (slash == std::string::npos || colon == std::string::npos)
                continue;
            entries.push_back(
                {std::stoi(raw_key.substr(slash + 1)), raw_val.substr(0, colon), std::stoi(raw_val.substr(colon + 1))});
        }
        return entries;
    }

    std::vector<Entry> EtcdCoordinator::get_entries() {
        auto entries = fetch_range(CURL_TIMEOUT_MS);
        known_state.clear();
        for (auto &e : entries)
            known_state[e.func_id] = e;
        return entries;
    }

    std::optional<WatchEvent> EtcdCoordinator::next_event(int timeout_ms) {
        if (!event_queue.empty()) {
            auto ev = event_queue.front();
            event_queue.pop_front();
            return ev;
        }

        std::vector<Entry> current_entries;
        try {
            current_entries = fetch_range(timeout_ms);
        } catch (const std::exception &e) {
            BOOST_LOG_TRIVIAL(warning) << "EtcdCoordinator::next_event() error: " << e.what();
            return std::nullopt;
        }

        std::map<int, Entry> current;
        for (auto &e : current_entries)
            current[e.func_id] = e;

        for (auto &[fid, e] : current) {
            auto it = known_state.find(fid);
            if (it == known_state.end() || it->second.address != e.address || it->second.port != e.port)
                event_queue.push_back({EventType::PUT, e});
        }
        for (auto &[fid, e] : known_state) {
            if (current.find(fid) == current.end())
                event_queue.push_back({EventType::DELETE, e});
        }

        known_state = std::move(current);

        if (!event_queue.empty()) {
            auto ev = event_queue.front();
            event_queue.pop_front();
            return ev;
        }
        return std::nullopt;
    }

} // namespace FMI::Comm
