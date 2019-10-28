// Copyright 2019 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "SizedMap.hpp"

#include <map>
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
 * Singleton Timeslice manager that DDScheduler uses to track timeslices completion and timeout
 */
class TimesliceManager
{
public:
    // Initialize the instance and retrieve it
    static TimesliceManager* get_instance(uint32_t compute_index, uint32_t input_connection_count,
	    std::string log_directory, bool enable_logging);

    // Get singleton instance
    static TimesliceManager* get_instance();

    // Update input connection count
    void update_input_connection_count(uint32_t input_connection_count);

    // Set the begin time to be used in logging
    void log_contribution_arrival(uint32_t connection_id, uint64_t timeslice);

    // Get last ordered completed timeslice
    uint64_t get_last_ordered_completed_timeslice();

    //Generate log files of the stored data
    void generate_log_files();

private:

    TimesliceManager(uint32_t compute_index, uint32_t input_connection_count,
	    std::string log_directory, bool enable_logging);

    void trigger_timeslice_completion (uint64_t timeslice);

    // Last received timeslice from each input connection
    SizedMap<uint32_t, uint64_t> last_received_timeslice_;

    // The first arrival time of each timeslice
    SizedMap<uint64_t, std::chrono::high_resolution_clock::time_point> timeslice_first_arrival_time_;

    SizedMap<uint64_t, uint32_t> timeslice_arrived_count_;

    // The singleton instance for this class
    static TimesliceManager* instance_;

    // Compute process index
    uint32_t compute_index_;

    // The number of input connections
    uint32_t input_connection_count_;

    // The log directory
    std::string log_directory_;

    bool enable_logging_;

    // LOGGING
    // Time to complete each timeslice
    SizedMap<uint64_t, double> timeslice_completion_duration_;

};
}
