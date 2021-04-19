// Copyright 2019 Farouk Salem <salem@zib.de>

#pragma once

#include "dfs/controller/SchedulerOrchestrator.hpp"
#include "dfs/controller/fault_tolerance/ComputeHeartbeatManager.hpp"
#include "dfs/controller/interval_manager/ComputeTimesliceManager.hpp"
#include "dfs/controller/interval_manager/DDScheduler.hpp"
#include "dfs/controller/load_balancer/DDLoadBalancerManager.hpp"
#include "dfs/logger/ComputeLoggerProxy.hpp"
#include "dfs/model/interval_manager/ComputeIntervalDataManager.hpp"
#include "dfs/model/interval_manager/ComputeProposedIntervalMetaData.hpp"
#include "dfs/model/interval_manager/InputIntervalMetaData.hpp"
#include "dfs/model/load_balancer/DDSLoadBalancerMessage.hpp"

namespace tl_libfabric {
/**
 *  Facade layer of Distributed Deterministic Scheduler for compute nodes
 */
class DDSchedulerOrchestrator : public SchedulerOrchestrator {
public:
  //// Common Methods
  // Initialize
  static void initialize(uint32_t scheduler_index,
                         uint32_t input_scheduler_count,
                         uint64_t init_heartbeat_timeout,
                         uint32_t timeout_history_size,
                         uint32_t timeout_factor,
                         uint32_t inactive_factor,
                         uint32_t inactive_retry_count,
                         uint32_t history_size,
                         uint32_t interval_length,
                         uint32_t speedup_difference_percentage,
                         uint32_t speedup_percentage,
                         uint32_t speedup_interval_count,
                         uint32_t balancer_difference_percentage,
                         uint32_t balancer_interval_count,
                         std::string log_directory,
                         bool enable_logging);

  // Set the input nodes count
  static void
  update_clock_offset(uint32_t input_index,
                      std::chrono::high_resolution_clock::time_point MPI_time,
                      const uint64_t median_latency = ConstVariables::ZERO,
                      const uint64_t interval_index = ConstVariables::ZERO);

  // Set the begin time to be used in logging
  static void
  set_begin_time(std::chrono::high_resolution_clock::time_point begin_time);

  // Generate log files of the stored data
  static void generate_log_files();

  // DDScheduler Methods
  // Receive actual interval meta-data from ComputeNodeConnection
  static void add_actual_meta_data(
      const uint32_t input_index,
      InputIntervalMetaData meta_data,
      ComputeIntervalMetaDataStatistics* prev_interval_compute_stats);

  // Return the proposed interval meta-data to ComputeNodeConnection
  static const ComputeProposedIntervalMetaData*
  get_proposed_meta_data(uint32_t input_index, uint64_t interval_index);

  // Return the  interval meta-data Statistics of a completed interval
  static const ComputeIntervalMetaDataStatistics*
  get_actual_meta_data_statistics(uint64_t interval_index);

  // Check what is the last completed interval
  static uint64_t get_last_completed_interval();

  //// ComputeTimesliceManager Methods

  // Set the begin time to be used in logging
  static void log_contribution_arrival(uint32_t connection_id,
                                       uint64_t timeslice);

  // Undo logging contribution arrival after rescheduling decision
  static bool undo_log_contribution_arrival(uint32_t connection_id,
                                            uint64_t timeslice);

  // Get last ordered completed timeslice
  static uint64_t get_last_ordered_completed_timeslice();

  // Check timeslices that should time out
  static void log_timeout_timeslice();

  // Check whether a timeslice is timed out
  static bool is_timeslice_timed_out(uint64_t timeslice);

  //// ComputeHeartbeatManager Methods

  // log the arrival of failure node message
  static HeartbeatFailedNodeInfo*
  log_heartbeat_failure(uint32_t connection_id,
                        HeartbeatFailedNodeInfo failure_info);

  // Check whether a decision of a failed connection is ready
  static bool is_failed_node_decision_ready(uint32_t failed_connection_id);

  // A list of input connections to inform about a compute node failure
  // <failed node, list of connections>
  static std::pair<uint32_t, std::set<uint32_t>>
  retrieve_missing_info_from_connections();

  // Get a decision about a particular failed compute node
  static HeartbeatFailedNodeInfo*
  get_decision_of_failed_connection(uint32_t failed_connection_id);

  // Log the acknowledge of receiving a decision
  static void log_decision_ack(uint32_t connection_id,
                               uint32_t failed_connection_id);

  // Check whether all decisions are acked
  static bool is_all_failure_decisions_acked();

  // Log when the finalize message is sent
  static void log_finalize_connection(uint32_t connection_id,
                                      bool ack_received = false);

  // Retrieve Connections that are not received any finalize ACK for a timeout
  // period
  static std::vector<uint32_t> retrieve_long_waiting_finalized_connections();

  // Add new pending heartbeat message
  static void add_pending_dfs_message(std::uint32_t connection_id,
                                      DDSLoadBalancerMessage* message);

  // Get one of the pending messages, if there
  static DDSLoadBalancerMessage*
  get_pending_dfs_message(std::uint32_t connection_id);

  //
  static void log_buffer_fill_level(std::vector<uint64_t> individual_levels);

  //
  static void log_timeslice_start_processing(uint64_t timeslice);

  //
  static void log_timeslice_processing_completion(uint64_t timeslice);

  // TODO TO BE REMOVED
  static bool SHOW_LOG_;

  //// Variables
private:
  static DDScheduler* interval_scheduler_;
  static ComputeTimesliceManager* timeslice_manager_;
  static ComputeHeartbeatManager* heartbeat_manager_;
  static ComputeIntervalDataManager* compute_interval_data_manager_;
  static DDLoadBalancerManager* load_balancer_manager_;
};
} // namespace tl_libfabric
