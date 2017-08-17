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
    // time to send a particular timeslice
    std::chrono::high_resolution_clock::time_point time_to_send;
    uint64_t timeslice_to_send = ConstVariables::MINUS_ONE;
    // duration between sending a contribution to another
    uint64_t duration;
};
}

#pragma pack()
