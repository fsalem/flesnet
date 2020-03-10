// Copyright 2019 Farouk Salem <salem@zib.de>

#pragma once

#include "SizedMap.hpp"
#include "HeartbeatMessage.hpp"
#include "ConstVariables.hpp"

#include <set>
#include <math.h>
#include <string>
#include <chrono>
#include <cassert>
#include <log.hpp>

#include <fstream>
#include <iomanip>


namespace tl_libfabric
{
/**
 * Singleton Heartbeat manager that DFS uses to detect the failure of a connection
 */
class HeartbeatManager
{
public:

    // Log sent heartbeat message
    void log_sent_heartbeat_message(uint32_t connection_id, HeartbeatMessage message);

    // get next message id sequence
    uint64_t get_next_heartbeat_message_id();

    // Log the time that the heartbeat message is acked
    void ack_message_received(uint64_t messsage_id);

    // Count the unacked messages of a particular connection
    uint32_t count_unacked_messages(uint32_t connection_id);

protected:

    struct HeartbeatMessageInfo{
	HeartbeatMessage message;
	std::chrono::high_resolution_clock::time_point transmit_time;
	std::chrono::high_resolution_clock::time_point completion_time;
	bool acked = false;
	uint32_t dest_connection;
    };

   HeartbeatManager(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging);

    // Compute process index
    uint32_t index_;

    // The number of input connections
    uint32_t connection_count_;

    // Sent message log <message_id, message_info>
    SizedMap<uint64_t, HeartbeatMessageInfo*> heartbeat_message_log_;

    // Not acked messages log
    SizedMap<uint32_t, std::set<uint64_t>*> unacked_sent_messages_;

    // The log directory
    std::string log_directory_;

    bool enable_logging_;
};
}
