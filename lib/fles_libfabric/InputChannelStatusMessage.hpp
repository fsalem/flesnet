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
    uint64_t in_acked_timestamp;
    uint64_t in_acked_timeslice = -1;
};
}

#pragma pack()
