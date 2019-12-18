// Copyright 2018 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "SizedMap.hpp"
#include "IntervalMetaData.hpp"
#include "InputIntervalInfo.hpp"

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
 * Singleton Scheduler for input nodes that could be used in InputChannelSender and InputChannelConnections!
 */
class InputIntervalScheduler
{
public:

    // Initialize and get singleton instance
    static InputIntervalScheduler* get_instance(uint32_t scheduler_index, uint32_t compute_conn_count,
	    uint32_t interval_length, std::string log_directory, bool enable_logging);

    // Get singleton instance
    static InputIntervalScheduler* get_instance();

    // update the compute node count which is needed for the initial interval (#0)
    void update_compute_connection_count(uint32_t);

    // Set the begin time to be used in logging and create the first interval if not there
    void update_input_begin_time(std::chrono::high_resolution_clock::time_point);

    // Receive proposed interval meta-data from InputChannelConnections
    void add_proposed_meta_data(const IntervalMetaData);

    // Return the actual interval meta-data to InputChannelConnections
    const IntervalMetaData* get_actual_meta_data(uint64_t);

    // Get last timeslice to be sent
    uint64_t get_last_timeslice_to_send();

    // Increase the sent timeslices by one
    void increament_sent_timeslices(uint64_t timeslice);

    // Undo the sent timeslices incremental
    void undo_increament_sent_timeslices(uint64_t timeslice_trigger, std::vector<uint64_t> undo_timeslices);

    // Increase the acked timeslices by one
    void increament_acked_timeslices(uint64_t);

    // Get the time to start sending more timeslices
    int64_t get_next_fire_time();

    // Get the number of current compute node connections
    uint32_t get_compute_connection_count();

    // Log the transmission time of a timeslice
    void log_timeslice_transmit_time(uint64_t timeslice, uint32_t);

    //Generate log files of the stored data
    void generate_log_files();

private:

    struct TimesliceInfo{
	std::chrono::high_resolution_clock::time_point expected_time;
	std::chrono::high_resolution_clock::time_point transmit_time;
	uint32_t compute_index;
	uint64_t acked_duration = 0;
    };

    InputIntervalScheduler(uint32_t scheduler_index, uint32_t compute_conn_count,
	    uint32_t interval_length, std::string log_directory, bool enable_logging);

    // The singleton instance for this class
    static InputIntervalScheduler* instance_;

    /// create a new interval with specific index
    void create_new_interval_info(uint64_t);

    // Create an entry in the actual interval meta-data list to be sent to compute schedulers
    void create_actual_interval_meta_data(InputIntervalInfo*);

    // Get the expected number of sent timeslices so far of a particular interval
    uint64_t get_expected_sent_ts_count(uint64_t);

    // Get the expected sent timeslice so far
    uint64_t get_expected_last_sent_ts(uint64_t);

    // Get the current round index of a particular interval
    uint64_t get_interval_current_round_index(uint64_t);

    // Get the expected round index of a particular interval based on round duration
    uint64_t get_interval_expected_round_index(uint64_t);

    // Get the interval info of a particular timeslice
    InputIntervalInfo* get_interval_of_timeslice(uint64_t);

    // Check whether all timeslices of a particular interval are sent out
    bool is_interval_sent_completed(uint64_t);

    // Check whether all timeslices of a particular interval are acked
    bool is_interval_sent_ack_completed(uint64_t);

    // Check whether a specific ack theshold is reached to speedup sending rate to catch others
    bool is_ack_percentage_reached(uint64_t);

    // Retrieve the time of the expected round of a particular interval based on the actual start time and duration
    std::chrono::high_resolution_clock::time_point get_interval_time_to_expected_round(uint64_t);

    std::chrono::high_resolution_clock::time_point get_expected_ts_sent_time(uint64_t interval, uint64_t timeslice);

    std::chrono::high_resolution_clock::time_point get_expected_round_sent_time(uint64_t interval, uint64_t round);

    // List of all interval infos
    SizedMap<uint64_t, InputIntervalInfo*> interval_info_;

    // Proposed interval meta-data
    SizedMap<uint64_t, IntervalMetaData*> proposed_interval_meta_data_;

    // Actual interval meta-data
    SizedMap<uint64_t, IntervalMetaData*> actual_interval_meta_data_;

    // The number of compute connections
    uint32_t compute_count_;

    // Input Scheduler index
    uint32_t scheduler_index_;

    // Time at which the InputChannelSender started
    std::chrono::high_resolution_clock::time_point begin_time_;

    // Number of intial timeslices per interval
    uint32_t interval_length_;

    // The Log folder
    std::string log_directory_;

    // Check whether to generate log files
    bool enable_logging_;

    /// LOGGING
    SizedMap<std::pair<uint64_t, uint64_t>, std::pair<uint64_t, uint64_t>> round_proposed_actual_start_time_log_;



};
}
