#include "../../include/comm/CheckpointablePeerToPeer.h"
#include "checkpoint.hpp"

void FMI::Comm::CheckpointablePeerToPeer::bcast(channel_data buf, FMI::Utils::peer_num root)
{
    checkpointer.check_should_checkpoint([&]()
                                         { teardown_fn(); }, [&]()
                                         { restore_fn(); }, 1);

    PeerToPeer::bcast(buf, root);
}

void FMI::Comm::CheckpointablePeerToPeer::barrier()
{
    checkpointer.check_should_checkpoint([&]()
                                         { teardown_fn(); }, [&]()
                                         { restore_fn(); }, 1);

    PeerToPeer::barrier();
}

void FMI::Comm::CheckpointablePeerToPeer::gather(channel_data sendbuf, channel_data recvbuf, FMI::Utils::peer_num root)
{
    checkpointer.check_should_checkpoint([&]()
                                         { teardown_fn(); }, [&]()
                                         { restore_fn(); }, 1);

    PeerToPeer::gather(sendbuf, recvbuf, root);
}

void FMI::Comm::CheckpointablePeerToPeer::scatter(channel_data sendbuf, channel_data recvbuf, FMI::Utils::peer_num root)
{
    checkpointer.check_should_checkpoint([&]()
                                         { teardown_fn(); }, [&]()
                                         { restore_fn(); }, 1);

    PeerToPeer::scatter(sendbuf, recvbuf, root);
}

void FMI::Comm::CheckpointablePeerToPeer::reduce(channel_data sendbuf, channel_data recvbuf, FMI::Utils::peer_num root, raw_function f)
{
    checkpointer.check_should_checkpoint([&]()
                                         { teardown_fn(); }, [&]()
                                         { restore_fn(); }, 1);

    PeerToPeer::reduce(sendbuf, recvbuf, root, f);
}

void FMI::Comm::CheckpointablePeerToPeer::allreduce(channel_data sendbuf, channel_data recvbuf, raw_function f)
{
    checkpointer.check_should_checkpoint([&]()
                                         { teardown_fn(); }, [&]()
                                         { restore_fn(); }, 1);

    PeerToPeer::allreduce(sendbuf, recvbuf, f);
}

void FMI::Comm::CheckpointablePeerToPeer::scan(channel_data sendbuf, channel_data recvbuf, raw_function f)
{
    checkpointer.check_should_checkpoint([&]()
                                         { teardown_fn(); }, [&]()
                                         { restore_fn(); }, 1);

    PeerToPeer::scan(sendbuf, recvbuf, f);
}
