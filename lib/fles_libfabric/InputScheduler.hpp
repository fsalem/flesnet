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

    // Receive proposed interval meta-data from InputChannelConnections
    void add_proposed_meta_data(IntervalMetaData*);

    // Return the actual interval meta-data to InputChannelConnections
    IntervalMetaData* get_actual_meta_data(uint64_t);

    // Get last timeslice to be sent
    uint64_t get_last_timeslice_to_send();

    // Increase the sent timeslices by one
    void increament_sent_timeslices();

    // Increase the acked timeslices by one
    void increament_acked_timeslices(uint64_t);

    // Get the time to start sending more timeslices
    std::chrono::system_clock::time_point get_next_fire_time();

    // TODO TO BE REMOVED
    static uint64_t COMPUTE_COUNT_;
private:
    InputScheduler();

    static InputScheduler* instance_;

    void create_new_interval_info(uint64_t);

    void create_actual_interval_meta_data(InputIntervalInfo*);

    uint64_t get_expected_sent_ts(uint64_t);

    uint64_t get_interval_current_round_index(uint64_t);

    uint64_t get_interval_expected_round_index(uint64_t);

    InputIntervalInfo* get_interval_of_timeslice(uint64_t);

    bool is_interval_sent_completed(uint64_t);

    bool is_interval_sent_ack_completed(uint64_t);

    bool is_ack_percentage_reached(uint64_t);

    std::chrono::system_clock::time_point get_interval_time_to_expected_round(uint64_t);

    std::chrono::system_clock::time_point get_expected_ts_sent_time(uint64_t, uint64_t);

    // List of all interval infos
    SizedMap<uint64_t, InputIntervalInfo*> interval_info_;

    // Proposed interval meta-data
    SizedMap<uint64_t, IntervalMetaData*> proposed_interval_meta_data_;

    // Actual interval meta-data
    SizedMap<uint64_t, IntervalMetaData*> actual_interval_meta_data_;



};
}
