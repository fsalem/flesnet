// Copyright 2019 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
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
 * Singleton Heart Beat manager that DFS uses to detect the failure of a connection
 */
class InputHeartbeatManager
{
public:
    // Initialize the instance and retrieve it
    static InputHeartbeatManager* get_instance(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging);

    // Get singleton instance
    static InputHeartbeatManager* get_instance();

    // Set the begin time to be used in logging
    void log_heartbeat(uint32_t connection_id);

    // Retrieve the inactive connections to send heartbeat message
    std::vector<uint32_t> retrieve_new_inactive_connections();

    // Retrieve a list of timeout connections
    std::vector<uint32_t> retrieve_new_timeout_connections();

    // Get a new timed out connection to send heartbeat message (-1 is returned if there is no)
    int32_t get_new_timeout_connection();

    // Check whether a connection is inactive
    bool is_connection_inactive(uint32_t connection_id);

    // Check whether a connection is already timedout
    bool is_connection_timed_out(uint32_t connection_id);

    // Mark connection as timedout
    void mark_connection_timed_out(uint32_t connection_id);

    // Log sent heartbeat message
    void log_sent_heartbeat_message(HeartbeatMessage message);

    // get next message id sequence
    uint64_t get_next_heartbeat_message_id();

    //Generate log files of the stored data
    //void generate_log_files();

private:

    InputHeartbeatManager(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging);


    // The singleton instance for this class
    static InputHeartbeatManager* instance_;

    // Compute process index
    uint32_t index_;

    // The number of input connections
    uint32_t connection_count_;

    // Time of the last received message from a connection
    std::vector<std::chrono::high_resolution_clock::time_point> connection_heartbeat_time_;

    // List of all the timed out connections
    std::set<uint32_t> timed_out_connection_;

    // List of the inactive connections
    std::set<uint32_t> inactive_connection_;

    // Sent message log
    std::set<HeartbeatMessage> heartbeat_message_log_;

    // Timeout limit in seconds
    double timeout_;

    // The log directory
    std::string log_directory_;

    bool enable_logging_;

    // LOGGING

};
}
