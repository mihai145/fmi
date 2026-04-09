#ifndef FMI_CHECKPOINTABLE_PEERTOPEER_H
#define FMI_CHECKPOINTABLE_PEERTOPEER_H

#include "PeerToPeer.h"
#include "checkpoint.hpp"

namespace FMI::Comm
{
    //! Checkpointable Peer-To-Peer channel type

    class CheckpointablePeerToPeer : public PeerToPeer
    {
    public:
        void bcast(channel_data buf, FMI::Utils::peer_num root) override;

        void barrier() override;

        void gather(channel_data sendbuf, channel_data recvbuf, FMI::Utils::peer_num root) override;

        void scatter(channel_data sendbuf, channel_data recvbuf, FMI::Utils::peer_num root) override;

        void reduce(channel_data sendbuf, channel_data recvbuf, FMI::Utils::peer_num root, raw_function f) override;

        void allreduce(channel_data sendbuf, channel_data recvbuf, raw_function f) override;

        void scan(channel_data sendbuf, channel_data recvbuf, raw_function f) override;

        // functions used for high-level cleanup/restore for C/R
        virtual void teardown_fn() = 0;
        virtual void restore_fn() = 0;

    protected:
        checkpoint::Checkpoint checkpointer;
    };
}

#endif // FMI_CHECKPOINTABLE_PEERTOPEER_H
