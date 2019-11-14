// Copyright 2018 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "SizedMap.hpp"

#include <vector>
#include <map>
#include <set>
#include <math.h>
#include <cstdint>
#include <string>
#include <chrono>
#include <cassert>
#include <iostream>
#include <log.hpp>


namespace tl_libfabric
{
/**
 * Singleton Input Timeslice Manager for input nodes that could be used in InputChannelSender and InputChannelConnections!
 */
class InputTimesliceManager
{
public:

    // Initialize and get singleton instance
    static InputTimesliceManager* get_instance(uint32_t scheduler_index, uint32_t compute_conn_count,
	    uint32_t interval_length, std::string log_directory, bool enable_logging);

    // Get singleton instance
    static InputTimesliceManager* get_instance();

    // update the compute node count which is needed for the initial interval (#0)
    void update_compute_connection_count(uint32_t);

    // Set the input scheduler index
    void update_input_scheduler_index(uint32_t);

    // Increase the sent timeslices by one
    void increament_sent_timeslices();

    // Increase the acked timeslices by one
    void increament_acked_timeslices(uint64_t);

    // Get the time to start sending more timeslices
    int64_t get_next_fire_time();

    // Get the number of current compute node connections
    uint32_t get_compute_connection_count();

    // Check whether a timeslice is acked
    bool is_timeslice_acked(uint64_t timeslice);

    // Log the transmission time of a timeslice
    void log_timeslice_transmit_time(uint64_t timeslice, uint32_t);

    // Log the duration of a timeslice until receiving the ack
    void log_timeslice_ack_time(uint64_t);

    //Generate log files of the stored data
    void generate_log_files();

    void log_timeslice_IB_blocked(uint64_t timeslice, bool sent_completed=false);

    void log_timeslice_CB_blocked(uint64_t timeslice, bool sent_completed=false);

    void log_timeslice_MR_blocked(uint64_t timeslice, bool sent_completed=false);

private:

    struct TimesliceInfo{
    	std::chrono::high_resolution_clock::time_point transmit_time;
    	uint32_t compute_index;
    	uint64_t acked_duration = 0;
    };

    InputTimesliceManager(uint32_t scheduler_index, uint32_t compute_conn_count,
	    uint32_t interval_length, std::string log_directory, bool enable_logging);

    // The singleton instance for this class
    static InputTimesliceManager* instance_;

    // The number of compute connections
    uint32_t compute_count_;

    // Input Scheduler index
    uint32_t scheduler_index_;

    // Number of intial timeslices per interval
    uint32_t interval_length_;

    // The Log folder
    std::string log_directory_;

    // Check whether to generate log files
    bool enable_logging_;

    /// LOGGING
    SizedMap<uint64_t, TimesliceInfo*> timeslice_info_log_;
    //Input Buffer blockage
    SizedMap<uint64_t, std::chrono::high_resolution_clock::time_point> timeslice_IB_blocked_start_log_;
    SizedMap<uint64_t, uint64_t> timeslice_IB_blocked_duration_log_;
    //Compute Buffer blockage
    SizedMap<uint64_t, std::chrono::high_resolution_clock::time_point> timeslice_CB_blocked_start_log_;
    SizedMap<uint64_t, uint64_t> timeslice_CB_blocked_duration_log_;
    //Max writes limitation blockage
    SizedMap<uint64_t, std::chrono::high_resolution_clock::time_point> timeslice_MR_blocked_start_log_;
    SizedMap<uint64_t, uint64_t> timeslice_MR_blocked_duration_log_;


};
}
