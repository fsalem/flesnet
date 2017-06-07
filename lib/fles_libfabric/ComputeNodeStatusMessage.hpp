// Copyright 2014 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "ComputeNodeBufferPosition.hpp"
#include "ComputeNodeInfo.hpp"
#include <chrono>

#pragma pack(1)

namespace tl_libfabric
{
/// Structure representing a status update message sent from compute buffer to
/// input channel.
struct ComputeNodeStatusMessage {
    ComputeNodeBufferPosition ack;
    bool request_abort;
    bool final;
    //
    bool connect;
    ComputeNodeInfo info;
    // address must be not null if connect = true
    unsigned char my_address[64];
    // last acked time difference from the barrier of predecessor of the target input node
    uint64_t in_acked_time;
    uint64_t in_acked_timeslice = -1;
};
}

#pragma pack()
