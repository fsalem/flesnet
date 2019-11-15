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

    // Get the next timeslice to be trasmitted to specific compute node
    uint64_t get_connection_next_timeslice(uint32_t compute_index);

    // Log the transmission time of a timeslice
    void log_timeslice_transmit_time(uint32_t compute_index, uint64_t timeslice);

    // log the duration to write a timeslice to the destination
    void acknowledge_timeslice_rdma_write(uint32_t compute_index, uint64_t timeslice);

    // mark the timeslices up to specific descriptor as completed the acked timeslices by one
    void acknowledge_timeslices_completion(uint32_t compute_index, uint64_t up_to_descriptor_id);

    // Check whether a timeslice is acked
    bool is_timeslice_rdma_acked(uint32_t compute_index, uint64_t timeslice);

    // Get the number of current compute node connections
    uint32_t get_compute_connection_count();

    // Get the last acked descriptor ID of a compute node
    uint64_t get_last_acked_descriptor(uint32_t compute_index);

    // Get the timeslice number of a specific descriptor
    uint64_t get_timeslice_of_not_acked_descriptor(uint32_t compute_index, uint64_t descriptor);

    //Generate log files of the stored data
    void generate_log_files();

    void log_timeslice_IB_blocked(uint64_t timeslice, bool sent_completed=false);

    void log_timeslice_CB_blocked(uint64_t timeslice, bool sent_completed=false);

    void log_timeslice_MR_blocked(uint64_t timeslice, bool sent_completed=false);

private:

    struct TimesliceInfo{
    	std::chrono::high_resolution_clock::time_point transmit_time;
    	uint64_t compute_desc;
    	uint64_t rdma_acked_duration = 0;
    	uint64_t completion_acked_duration = 0;
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

    SizedMap<uint32_t, SizedMap<uint64_t, TimesliceInfo*>*> conn_timeslice_info_;

    // Mapping of timeslice and its descriptor ID of each compute node
    SizedMap<uint32_t, SizedMap<uint64_t, uint64_t>*> conn_desc_timeslice_info_;

    /// LOGGING
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
