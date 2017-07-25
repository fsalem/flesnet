// Copyright 2014 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
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
    // last acked timeslice from predecessor and successor
    uint64_t acked_timeslice = MINUS_ONE;
    // last acked time difference from the barrier of predecessor of the target input node
    uint64_t predecessor_acked_time;
    // last acked time difference from the barrier of successor of the target input node
	uint64_t successor_acked_time;
};
}

#pragma pack()
