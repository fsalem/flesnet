// Copyright 2019 Farouk Salem <salem@zib.de>

#pragma once

#include "SizedMap.hpp"
#include "HeartbeatMessage.hpp"

#include <vector>
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

protected:

    struct HeartbeatMessageInfo{
	HeartbeatMessage message;
	std::chrono::high_resolution_clock::time_point transmit_time;
	bool acked = false;
	uint32_t dest_connection;

	bool operator< (const HeartbeatMessageInfo &right) const
	{
	    return message.message_id < right.message.message_id;
	}

	bool operator> (const HeartbeatMessageInfo &right) const
	{
	    return message.message_id > right.message.message_id;
	}

	bool operator== (const HeartbeatMessageInfo &right) const
	{
	    return message.message_id == right.message.message_id;
	}
    };

   HeartbeatManager(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging);

    // Compute process index
    uint32_t index_;

    // The number of input connections
    uint32_t connection_count_;

    // Sent message log
    std::set<HeartbeatMessageInfo> heartbeat_message_log_;

    // Pending heartbeat messages to send


    // The log directory
    std::string log_directory_;

    // TODO
    // Max history size of the logs
    uint64_t max_history_size_;

    bool enable_logging_;
};
}
