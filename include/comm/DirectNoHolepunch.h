#ifndef FMI_DIRECT_NO_HOLEPUNCH_H
#define FMI_DIRECT_NO_HOLEPUNCH_H

#include "PeerToPeer.h"
#include "checkpoint.hpp"

#include <chrono>

namespace FMI::Comm
{
    const auto TCP_CONNECT_BACKOFF = std::chrono::milliseconds(100);
    
    class DirectNoHolepunch : public PeerToPeer
    {
    public:
        explicit DirectNoHolepunch(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params);
        ~DirectNoHolepunch();

        void send_object(channel_data buf, Utils::peer_num rcpt_id) override;

        void recv_object(channel_data buf, Utils::peer_num sender_id) override;

        double get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override
        {
            return -1.; // not relevant
        }

        double get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override
        {
            return -1.; // not relevant
        }

    private:
        checkpoint::Checkpoint checkpointer;

        std::vector<checkpoint::peer_details> peers;

        int listen_sock;
        std::vector<int> sockets;

        void initialize_state();
        void teardown_state();
        void check_socket(Utils::peer_num partner_id);
    };
}

#endif // FMI_DIRECT_NO_HOLEPUNCH_H
