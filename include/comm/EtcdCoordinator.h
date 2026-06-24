#ifndef FMI_ETCD_COORDINATOR_H
#define FMI_ETCD_COORDINATOR_H

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace FMI::Comm {

    struct Entry {
        int func_id;
        std::string address;
        int port;
    };

    enum class EventType { PUT, DELETE };

    struct WatchEvent {
        EventType type;
        Entry entry;
    };

    class Coordinator {
      public:
        virtual ~Coordinator() = default;

        virtual void advertise_own_key(int func_id, int port) = 0;
        virtual void delete_own_key(int func_id) = 0;
        virtual std::vector<Entry> get_entries() = 0;
        virtual std::optional<WatchEvent> next_event(int timeout_ms) = 0;
        virtual void start_watch() = 0;
        virtual void stop_watch() = 0;
    };

    class EtcdCoordinator : public Coordinator {
      public:
        EtcdCoordinator(const std::string &etcd_host, int etcd_port, const std::string &comm_name);
        ~EtcdCoordinator() override;

        void advertise_own_key(int func_id, int port) override;
        void delete_own_key(int func_id) override;
        std::vector<Entry> get_entries() override;
        std::optional<WatchEvent> next_event(int timeout_ms) override;

        void start_watch() override;
        void stop_watch() override;

      private:
        std::string base_url;
        std::string key_prefix;
        long last_revision{0};

        std::vector<Entry> fetch_range(int timeout_ms);

        // watch thread
        void watch_loop();
        void on_watch_data(const char *data, size_t len);
        std::vector<WatchEvent> parse_watch_events(const std::string &line);
        static size_t watch_write_cb(char *ptr, size_t size, size_t nmemb, void *ud);

        std::thread watch_thread;
        std::atomic<bool> watch_stop{false};
        std::mutex watch_multi_mutex;
        void *watch_multi{nullptr}; // CURLM*, guarded by watch_multi_mutex (for curl_multi_wakeup)
        std::string watch_buf;      // partial-line buffer

        std::mutex queue_mutex;
        std::condition_variable queue_cv;
        std::deque<WatchEvent> event_queue;
    };

} // namespace FMI::Comm

#endif // FMI_ETCD_COORDINATOR_H
