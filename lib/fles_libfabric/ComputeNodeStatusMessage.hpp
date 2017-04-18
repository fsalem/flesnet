// Copyright 2014 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "ComputeNodeBufferPosition.hpp"
#include "ComputeNodeInfo.hpp"

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
    // waiting time between calls in microseconds
    long int wait_time = 100;

};
}

#pragma pack()
