// Copyright 2014 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "ComputeNodeBufferPosition.hpp"
#include "InputNodeInfo.hpp"

#include <chrono>

#pragma pack(1)

namespace tl_libfabric
{
/// Structure representing a status update message sent from input channel to
/// compute buffer.
struct InputChannelStatusMessage {
    ComputeNodeBufferPosition wp;
    bool abort;
    bool final;
    // "private data" on connect
    bool connect;
    InputNodeInfo info;
    unsigned char my_address[64]; // gni: 50?};
    // Time of the MPI barrier
    std::chrono::high_resolution_clock::time_point MPI_time;
    // last acked time difference from the barrier of predecessor of the target input node
    std::chrono::high_resolution_clock::time_point sent_time;
    // The duration needed to send the contribution of the timeslice till receiving the acknowledgment
    uint64_t sent_duration;
    uint64_t sent_timeslice = MINUS_ONE;
};
}

#pragma pack()
