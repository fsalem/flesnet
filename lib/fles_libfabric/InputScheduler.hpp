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
class InputScheduler
{
public:
    // Get singleton instance
    static InputScheduler* get_instance();

    // Create the first initial interval and set the compute nodes count
    void initial_input_scheduler(uint32_t scheduler_index , uint32_t compute_conn_count, std::chrono::system_clock::time_point begin_time);

    // update the compute node count which is needed for the initial interval (#0)
    void update_compute_connection_count(uint32_t);

    // Set the input scheduler index
    void update_input_scheduler_index(uint32_t);

    // Set the begin time to be used in logging
    void update_input_begin_time(std::chrono::system_clock::time_point);

    // Receive proposed interval meta-data from InputChannelConnections
    void add_proposed_meta_data(const IntervalMetaData);

    // Return the actual interval meta-data to InputChannelConnections
    const IntervalMetaData* get_actual_meta_data(uint64_t);

    // Get last timeslice to be sent
    uint64_t get_last_timeslice_to_send();

    // Increase the sent timeslices by one
    void increament_sent_timeslices();

    // Increase the acked timeslices by one
    void increament_acked_timeslices(uint64_t);

    // Get the time to start sending more timeslices
    std::chrono::system_clock::time_point get_next_fire_time();

    // Log the transmission time of a timeslice
    void log_timeslice_transmit_time(uint64_t timeslice, uint32_t);

    // Log the duration of a timeslice until receiving the ack
    void log_timeslice_ack_time(uint64_t);

    //Generate log files of the stored data
    void generate_log_files();

private:

    struct TimesliceInfo{
	std::chrono::system_clock::time_point expected_time;
	std::chrono::system_clock::time_point transmit_time;
	uint32_t compute_index;
	uint64_t acked_duration;
    };

    InputScheduler();

    // The singleton instance for this class
    static InputScheduler* instance_;

    /// create a new interval with specific index
    void create_new_interval_info(uint64_t);

    // Create an entry in the actual interval meta-data list to be sent to compute schedulers
    void create_actual_interval_meta_data(InputIntervalInfo*);

    // Get the expected number of sent timeslices so far of a particular interval
    uint64_t get_expected_sent_ts(uint64_t);

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
    std::chrono::system_clock::time_point get_interval_time_to_expected_round(uint64_t);

    std::chrono::system_clock::time_point get_expected_ts_sent_time(uint64_t, uint64_t);

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

    std::chrono::system_clock::time_point begin_time_;

    /// LOGGING
    SizedMap<uint64_t, TimesliceInfo*> timeslice_info_log_;


};
}
