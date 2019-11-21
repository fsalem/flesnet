// Copyright 2019 Farouk Salem <salem@zib.de>

#pragma once

#include "InputNodeInfo.hpp"
#include "HeartbeatFailedNodeInfo.hpp"
#include "ConstVariables.hpp"

#include <chrono>

#pragma pack(1)

namespace tl_libfabric
{
/// Structure representing a heartbeat update message sent between input and compute channels
struct HeartbeatMessage {
    InputNodeInfo info;
    // Heart beat index
    uint64_t message_id = ConstVariables::ZERO;
    // Is this message an ack of a received message
    bool ack = false;
    // The info of a failed node
    HeartbeatFailedNodeInfo failure_info;
};
}

#pragma pack()
