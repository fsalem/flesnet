// Copyright 2019 Farouk Salem <salem@zib.de>

#pragma once

#include "InputIntervalScheduler.hpp"
#include "InputTimesliceManager.hpp"
#include "HeartbeatManager.hpp"
#include "IntervalMetaData.hpp"

namespace tl_libfabric
{
/**
 * Singleton and Facade layer of the Input Scheduler that could be used in InputChannelSender and InputChannelConnections
 */
class InputSchedulerOrchestrator
{
public:

    // Initialize the instance scheduler
    static void initialize(uint32_t scheduler_index, uint32_t compute_conn_count,
	    uint32_t interval_length, std::string log_directory, bool enable_logging);


    // update the compute node count which is needed for the initial interval (#0)
    static void update_compute_connection_count(uint32_t);

    // Set the input scheduler index
    static void update_input_scheduler_index(uint32_t);

    // Set the begin time to be used in logging and create the first interval if not there
    static void update_input_begin_time(std::chrono::high_resolution_clock::time_point);

    // Receive proposed interval meta-data from InputChannelConnections
    static void add_proposed_meta_data(const IntervalMetaData);

    // Return the actual interval meta-data to InputChannelConnections
    static const IntervalMetaData* get_actual_meta_data(uint64_t);

    // Get last timeslice to be sent
    static uint64_t get_last_timeslice_to_send();

    // Increase the sent timeslices by one
    static void increament_sent_timeslices();

    // Increase the acked timeslices by one
    static void increament_acked_timeslices(uint64_t);

    // Get the time to start sending more timeslices
    static int64_t get_next_fire_time();

    // Get the number of current compute node connections
    static uint32_t get_compute_connection_count();

    // Check whether a timeslice is acked
    static bool is_timeslice_acked(uint64_t timeslice);

    // Log the transmission time of a timeslice
    static void log_timeslice_transmit_time(uint64_t timeslice, uint32_t);

    // Log the duration of a timeslice until receiving the ack
    static void log_timeslice_ack_time(uint64_t);

    //Generate log files of the stored data
    static void generate_log_files();

    static void log_timeslice_IB_blocked(uint64_t timeslice, bool sent_completed=false);

    static void log_timeslice_CB_blocked(uint64_t timeslice, bool sent_completed=false);

    static void log_timeslice_MR_blocked(uint64_t timeslice, bool sent_completed=false);

    static void log_heartbeat(uint32_t connection_id);

private:
    static InputIntervalScheduler* interval_scheduler_;
    static InputTimesliceManager* timeslice_manager_;
    static HeartbeatManager* heartbeat_manager_;
};
}
