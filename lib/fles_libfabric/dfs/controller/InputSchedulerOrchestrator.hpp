// Copyright 2019 Farouk Salem <salem@zib.de>

#pragma once

#include "dfs/controller/SchedulerOrchestrator.hpp"
#include "dfs/controller/fault_tolerance/InputHeartbeatManager.hpp"
#include "dfs/controller/interval_manager/InputIntervalScheduler.hpp"
#include "dfs/controller/interval_manager/InputTimesliceManager.hpp"
#include "dfs/logger/InputLoggerProxy.hpp"
#include "dfs/model/fault_tolerance/HeartbeatFailedNodeInfo.hpp"
#include "dfs/model/interval_manager/ComputeProposedIntervalMetaData.hpp"
#include "dfs/model/interval_manager/InputIntervalMetaData.hpp"
#include "dfs/model/load_balancer/IELoadBalancerMessage.hpp"

// TODO remove
#include "DualRingBuffer.hpp"
#include "RingBuffer.hpp"

namespace tl_libfabric {
/**
 * Singleton and Facade layer of the Input Scheduler that could be used in
 * InputChannelSender and InputChannelConnections
 */
class InputSchedulerOrchestrator : public SchedulerOrchestrator {
public:
  ////Common Methods
  // Initialize the instance scheduler
  static void initialize(std::uint32_t scheduler_index,
                         std::uint32_t compute_conn_count,
                         uint64_t init_heartbeat_timeout,
                         uint32_t timeout_history_size,
                         uint32_t timeout_factor,
                         uint32_t inactive_factor,
                         uint32_t inactive_retry_count,
                         std::uint32_t interval_length,
                         uint64_t data_source_desc,
                         uint64_t desc_length,
                         uint64_t start_index_desc,
                         uint64_t timeslice_size,
                         std::string log_directory,
                         bool enable_logging);

  // update the compute node count which is needed for the initial interval
  // (#0)
  static void update_compute_connection_count(std::uint32_t);

  // Set the begin time to be used in logging and create the first interval if
  // not there
  static void
      update_input_begin_time(std::chrono::high_resolution_clock::time_point);

  // Get the number of current compute node connections
  static std::uint32_t get_compute_connection_count();

  // Generate log files of the stored data
  static void generate_log_files();

  //// InputIntervalScheduler Methods

  // Receive the DFS proposal from DDS
  static void add_dfs_proposal(const DDSLoadBalancerMessage,
                               uint32_t compute_index);

  // Receive proposed interval meta-data from InputChannelConnections
  static void add_proposed_meta_data(const ComputeProposedIntervalMetaData,
                                     const ComputeIntervalMetaDataStatistics,
                                     uint32_t compute_index);

  // Return the actual interval meta-data to InputChannelConnections
  static const InputIntervalMetaData* get_actual_meta_data(std::uint64_t);

  //
  static void retrieve_collected_interval_statistics(
      uint64_t interval_index, ComputeIntervalMetaDataStatistics* statistics);

  // Get last timeslice to be sent
  static std::uint64_t get_last_timeslice_to_send();

  // Get the time to start sending more timeslices
  static int64_t get_next_fire_time();

  // Get interval index of a particular timeslice
  static uint64_t get_interval_index_of_timeslice(uint64_t timeslice);

  // Get current interval index
  static uint64_t get_current_interval_index();

  // Get expected time to send timeslice
  static std::chrono::high_resolution_clock::time_point
  get_expected_ts_sent_time(uint64_t timeslice);

  //// InputTimesliceManager Methods

  // Get the timeslice to be sent to specific compute index
  static std::uint64_t
  get_connection_next_timeslice(std::uint32_t compute_index);

  // Log the transmission time of a timeslice
  static void mark_timeslice_transmitted(std::uint32_t compute_index,
                                         std::uint64_t timeslice,
                                         std::uint64_t size);

  // Log the duration to receive the completion event of rdma write operation
  static bool mark_timeslice_rdma_write_acked(std::uint32_t compute_index,
                                              std::uint64_t timeslice);

  // Log the duration of a timeslice until receiving the ack
  static void mark_timeslices_acked(std::uint32_t compute_index,
                                    std::uint64_t up_to_descriptor_id);

  // Check whether a timeslice is acked
  static bool is_timeslice_rdma_acked(std::uint32_t compute_index,
                                      std::uint64_t timeslice);

  // Get the last acked descriptor ID of a compute node
  static std::uint64_t get_last_acked_descriptor(std::uint32_t compute_index);

  // Get the timeslice number of a specific descriptor
  static std::uint64_t get_timeslice_by_descriptor(std::uint32_t compute_index,
                                                   std::uint64_t descriptor);

  // Get last descriptor of a connection
  static std::uint64_t
  get_last_connection_descriptor_index(std::uint32_t compute_index);

  // Return the data size and the descriptor index of a timeslice
  static std::pair<std::uint64_t, std::uint64_t>
  get_data_and_desc_of_timeslice(std::uint32_t compute_index,
                                 std::uint32_t timeslice);

  // Return the data size and the descriptor index of the last timeslice
  static std::pair<std::uint64_t, std::uint64_t>
  get_data_and_desc_of_last_timeslice(std::uint32_t compute_index);

  // Return the data size and the descriptor index of the last rdma acked
  // timeslice
  static std::pair<std::uint64_t, std::uint64_t>
  get_data_and_desc_of_last_rdma_acked_timeslice(std::uint32_t compute_index);

  // Calculate the last possible timeslice to be sent before blockage when a
  // connection timed out
  static void
  consider_reschedule_decision(HeartbeatFailedNodeInfo failed_node_info);

  // Check whether the decision message is already received
  static bool is_decision_considered(std::uint32_t connection_id);

  // Update the datasource desc to be used in case of node failure
  static void update_data_source_desc(uint64_t data_source_desc);

  // Get the # of sent timeslices
  static uint64_t get_sent_timeslices();

  // Add new pending DFS-LB message
  static void add_pending_dfs_message(std::uint32_t connection_id,
                                      IELoadBalancerMessage* message);

  // Get one of the pending messages, if there
  static IELoadBalancerMessage*
  get_pending_dfs_message(std::uint32_t connection_id);

  //
  static void log_timeslice_IB_blocked(std::uint32_t compute_index,
                                       std::uint64_t timeslice,
                                       bool sent_completed = false);

  //
  static void log_timeslice_CB_blocked(std::uint32_t compute_index,
                                       std::uint64_t timeslice,
                                       bool sent_completed = false);

  //
  static void log_timeslice_MR_blocked(std::uint32_t compute_index,
                                       std::uint64_t timeslice,
                                       bool sent_completed = false);

  //// Methods combine data from different objects
  static HeartbeatFailedNodeInfo*
  get_timed_out_connection(int32_t timeout_conn = -1);

  // TODO TO BE REMOVED
  static bool SHOW_LOGS_;
  //////
private:
  static InputIntervalScheduler* interval_scheduler_;
  static InputTimesliceManager* timeslice_manager_;
  static InputHeartbeatManager* heartbeat_manager_;
};
} // namespace tl_libfabric
