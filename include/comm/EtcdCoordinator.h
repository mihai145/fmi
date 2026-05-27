#ifndef FMI_ETCD_COORDINATOR_H
#define FMI_ETCD_COORDINATOR_H

#include <deque>
#include <map>
#include <optional>
#include <string>
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
    };

    class EtcdCoordinator : public Coordinator {
      public:
        EtcdCoordinator(const std::string &etcd_host, int etcd_port, const std::string &comm_name);
        void advertise_own_key(int func_id, int port) override;
        void delete_own_key(int func_id) override;
        std::vector<Entry> get_entries() override;
        std::optional<WatchEvent> next_event(int timeout_ms) override;

      private:
        std::string base_url;
        std::string key_prefix;
        std::map<int, Entry> known_state;
        std::deque<WatchEvent> event_queue;

        std::vector<Entry> fetch_range(int timeout_ms);
    };

} // namespace FMI::Comm

#endif // FMI_ETCD_COORDINATOR_H
