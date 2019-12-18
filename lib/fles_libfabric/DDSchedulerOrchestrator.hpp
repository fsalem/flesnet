// Copyright 2019 Farouk Salem <salem@zib.de>

#pragma once

#include "IntervalMetaData.hpp"
#include "DDScheduler.hpp"
#include "ComputeTimesliceManager.hpp"
#include "ComputeHeartbeatManager.hpp"


namespace tl_libfabric
{
/**
 *  Facade layer of Distributed Deterministic Scheduler for compute nodes
 */
class DDSchedulerOrchestrator
{
public:
//// Common Methods
    // Initialize
    static void initialize(uint32_t scheduler_index,
	    uint32_t input_connection_count,
	    uint32_t history_size,
	    uint32_t interval_duration,
	    uint32_t speedup_difference_percentage,
	    uint32_t speedup_percentage,
	    uint32_t speedup_interval_count,
	    std::string log_directory, bool enable_logging);


    // Set the input nodes count
    static void update_clock_offset(uint32_t input_index, std::chrono::high_resolution_clock::time_point MPI_time, const uint64_t median_latency = ConstVariables::ZERO, const uint64_t interval_index = ConstVariables::ZERO);

    // Set the begin time to be used in logging
    static void set_begin_time(std::chrono::high_resolution_clock::time_point begin_time);

    //Generate log files of the stored data
    static void generate_log_files();

// DDScheduler Methods
    // Receive actual interval meta-data from ComputeNodeConnection
    static void add_actual_meta_data(const uint32_t input_index, IntervalMetaData meta_data);

    // Return the proposed interval meta-data to ComputeNodeConnection
    static const IntervalMetaData* get_proposed_meta_data(uint32_t input_index, uint64_t interval_index);

    // Check what is the last completed interval
    static uint64_t get_last_completed_interval();

//// ComputeTimesliceManager Methods

    // Set the begin time to be used in logging
    static void log_contribution_arrival(uint32_t connection_id, uint64_t timeslice);

    // Undo logging contribution arrival after rescheduling decision
    static bool undo_log_contribution_arrival(uint32_t connection_id, uint64_t timeslice);

    // Get last ordered completed timeslice
    static uint64_t get_last_ordered_completed_timeslice();

    // Check timeslices that should time out
    static void log_timeout_timeslice();

    // Check whether a timeslice is timed out
    static bool is_timeslice_timed_out(uint64_t timeslice);

//// ComputeHeartbeatManager Methods

    // log the arrival of failure node message
    static void log_heartbeat_failure(uint32_t connection_id, HeartbeatFailedNodeInfo failure_info);

    // A list of input connections to inform about a compute node failure <failed node, list of connections>
    static std::pair<uint32_t, std::set<uint32_t>> retrieve_missing_info_from_connections();

    // Get a decision about a failed compute node to broadcast to input nodes
    static HeartbeatFailedNodeInfo* get_decision_to_broadcast();

    // Log the acknowledge of receiving a decision
    static void log_decision_ack(uint32_t connection_id);

    // Log when the finalize message is sent
    static void log_finalize_connection(uint32_t connection_id, bool ack_received = false);

    // Retrieve Connections that are not received any finalize ACK for a timeout period
    static std::vector<uint32_t> retrieve_long_waiting_finalized_connections();

    // TODO TO BE REMOVED
    static bool SHOW_LOG_;

//// Variables
private:
    static DDScheduler* interval_scheduler_;
    static ComputeTimesliceManager* timeslice_manager_;
    static ComputeHeartbeatManager* heartbeat_manager_;

};
}
