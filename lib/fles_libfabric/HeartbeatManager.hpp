// Copyright 2019 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "SizedMap.hpp"

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
class HeartbeatManager
{
public:
    // Initialize the instance and retrieve it
    static HeartbeatManager* get_instance(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging);

    // Get singleton instance
    static HeartbeatManager* get_instance();

    // Set the begin time to be used in logging
    void log_heartbeat(uint32_t connection_id);

    //
    std::vector<uint32_t> retrieve_timeout_connections();

    //
    bool is_connection_timed_out(uint32_t connection_id);

    //Generate log files of the stored data
    //void generate_log_files();

private:

    HeartbeatManager(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging);


    // The singleton instance for this class
    static HeartbeatManager* instance_;

    // Compute process index
    uint32_t index_;

    // The number of input connections
    uint32_t connection_count_;

    //
    std::vector<std::chrono::high_resolution_clock::time_point> connection_heartbeat_time_;

    //
    std::set<uint32_t> connection_timed_out_;

    // Timeout limit in seconds
    double timeout_;

    // The log directory
    std::string log_directory_;

    bool enable_logging_;

    // LOGGING

};
}
