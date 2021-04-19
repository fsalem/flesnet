// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "SizedMap.hpp"
#include "dfs/StatisticsCalculator.hpp"
#include "dfs/model/interval_manager/ComputeIntervalDataManager.hpp"
#include "dfs/model/load_balancer/ComputeIntervalMetaDataStatistics.hpp"
#include "log.hpp"

#include <cassert>
#include <chrono>
#include <cmath>
#include <vector>

namespace tl_libfabric {
/**
 * Singleton load balancer manager that determine the slow nodes and
 * redistribute load accordingly
 */
class DDLoadBalancerManager {
public:
  // Initialize the instance and retrieve it
  static DDLoadBalancerManager* get_instance(uint32_t scheduler_index, //
                                             uint32_t input_connection_count,
                                             uint32_t compute_connection_count,
                                             uint32_t max_history_size,
                                             uint32_t variance_percentage,
                                             std::string log_directory,
                                             bool enable_logging);

  // Get an already-initialized instance
  static DDLoadBalancerManager* get_instance();

  // Get the last load distribution of compute nodes
  std::vector<double>
  get_last_distribution_load(); // Calculate a new load distribution after a
                                // speedup phase

  // Check whether a load redistribution is needed
  bool needs_redistribute_load(uint64_t interval_index,
                               uint64_t speedup_start_interval_index,
                               uint64_t speedup_end_interval_index);

  std::vector<double>
  calculate_new_distribtion_load(uint64_t interval_index,
                                 uint64_t speedup_start_interval_index,
                                 uint64_t speedup_end_interval_index);

  std::vector<double>
  consider_new_distribtion_load(uint64_t interval_index,
                                uint64_t speedup_start_interval_index,
                                uint64_t speedup_end_interval_index);

  // Generate log files of the stored data
  void generate_log_files();

private:
  DDLoadBalancerManager(uint32_t scheduler_index,
                        uint32_t input_connection_count,
                        uint32_t compute_connection_count,
                        uint32_t max_history_size,
                        uint32_t variance_percentage,
                        std::string log_directory,
                        bool enable_logging);

  // TODO REMOVE LOGS
  typedef enum { RDMA_LATENCY, MSG_LATENCY, TS_COMP, TS_PROC } MatrixType;
  struct MatrixLog {
    StatisticsCalculator::ListStatistics stats;
    // std::vector<uint64_t> median_values;
    double variance;
    double load_to_distribute = 0;
  };

  ////////////////////// Business Logic //////////////////////
  // Get load distribution based on TS processing duration of compute node
  // during a speedup phase
  std::pair<bool, std::vector<double>>
  get_load_by_ts_processing_duration(uint64_t interval_index,
                                     uint64_t speedup_start_interval_index,
                                     uint64_t speedup_end_interval_index,
                                     bool calculate = true);

  // Get load distribution based on TS completion duration of compute node
  // during a speedup phase
  std::pair<bool, std::vector<double>>
  get_load_by_ts_completion_duration(uint64_t interval_index,
                                     uint64_t speedup_start_interval_index,
                                     uint64_t speedup_end_interval_index,
                                     bool calculate = true);

  // Get load distribution based on the buffer fill level
  /*std::vector<double>
  get_load_by_buffer_fill_level(uint64_t speedup_start_interval_index,
                                uint64_t speedup_end_interval_index);*/

  // Get the load distribution based on the RDMA writes latency observed at
  // Input nodes for each compute node
  std::pair<bool, std::vector<double>>
  get_load_by_rdma_latency(uint64_t interval_index,
                           uint64_t speedup_start_interval_index,
                           uint64_t speedup_end_interval_index,
                           bool calculate = true);

  // Get the load distribution based on the message latency observed at
  // Input nodes for each compute node
  std::pair<bool, std::vector<double>>
  get_load_by_msg_latency(uint64_t interval_index,
                          uint64_t speedup_start_interval_index,
                          uint64_t speedup_end_interval_index,
                          bool calculate = true);

  ////////////////////// Common Business Logic //////////////////////
  /**
   * Return <new_load, load>; when new_load is false, then last load is returned
   */
  std::pair<bool, std::vector<double>>
  get_load_by_lowering_duration(std::vector<uint64_t> median_duration,
                                MatrixLog* log,
                                bool calculate = true);

  ////////////////////// Helper Methods /////////////////////////

  // Calculate the median TS processing/completion duration of compute nodes
  // from start_interval_index to end_interval_index intervals inclusive
  std::vector<uint64_t>
  retrieve_median_ts_duration(uint64_t start_interval_index,
                              uint64_t end_interval_index,
                              bool processing_duration);

  // Calculate the median RDMA write latency of compute nodes from
  // start_interval_index to end_interval_index intervals inclusive
  std::vector<uint64_t>
  retrieve_median_input_latency(uint64_t start_interval_index,
                                uint64_t end_interval_index,
                                bool rdma_write);

  // Takes 2D array and calculates the median of each array
  std::vector<uint64_t> calculate_median_normalization(
      std::vector<std::vector<uint64_t>> duration_history);

  // Calculate aggregated average load distribution out of a set of
  // distributions
  std::vector<double> calculate_aggregated_average_load_distribution(
      std::vector<std::vector<double>> loads);

  // LOGS

  void generate_matrix_stats_logs(MatrixType type);
  ////////////////////// TO BE UPDATED //////////////////////
  /*
    //
    uint32_t find_blockgable_compute_node_by_buffer_blockage_time();

    //
    uint32_t get_slowest_input_node_of_slow_compute_node_using_blockage_time(
        uint32_t compute_index);

    // Calculate averaged CB blockage time of intervals of compute nodes
    void calculate_average_blockage_time_of_compute_nodes(
        ComputeCompletedIntervalMetaData* meta_data,
        std::vector<uint64_t>& blockage_time_to_add,
        bool IB_blockage);

    //
    uint64_t calculate_average_blockage_time_of_compute_index(
        ComputeCompletedIntervalMetaData* meta_data,
        uint32_t compute_indx,
        bool IB_blockage);

    //
    void calculate_sum_blockage_time_of_input_nodes(
        ComputeCompletedIntervalMetaData* meta_data,
        std::vector<uint64_t>& sum_blockage_time_to_add,
        uint32_t compute_indx,
        bool IB_blockage);
    */
  //
  uint32_t scheduler_index_;

  //
  uint32_t input_connection_count_;
  //
  uint32_t compute_connection_count_;

  //
  uint32_t max_history_size_;

  //
  uint32_t variance_percentage_;

  // The log directory
  std::string log_directory_;

  //
  bool enable_logging_;

  ComputeIntervalDataManager* compute_interval_data_manager_;

  // Log of load distribution <start_interval_index, distribution>
  SizedMap<uint64_t, std::vector<double>> interval_load_distribution_;

  //
  static DDLoadBalancerManager* instance_;

  // TODO REMOVE LOGS

  SizedMap<std::pair<uint64_t, MatrixType>, MatrixLog*> load_balancing_logs_;
};
} // namespace tl_libfabric
