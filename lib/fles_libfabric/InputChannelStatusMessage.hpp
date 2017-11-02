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
    /// TODO seperate message for the following to minimize the message size!!
    std::chrono::high_resolution_clock::time_point MPI_time;

    /// The acked interval
    uint64_t acked_interval_index = ConstVariables::MINUS_ONE;
    // The actual start time for the acked interval
    std::chrono::high_resolution_clock::time_point actual_start_time;
    // The proposed start time for the acked interval
    std::chrono::high_resolution_clock::time_point proposed_start_time;
    // The taken duration for the whole interval
    uint64_t interval_duration;

    /// The required interval info
    uint64_t required_interval_index = ConstVariables::MINUS_ONE;
};
}

#pragma pack()
