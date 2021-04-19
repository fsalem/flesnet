// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "DDLoadBalancerManager.hpp"

namespace tl_libfabric {

DDLoadBalancerManager*
DDLoadBalancerManager::get_instance(uint32_t scheduler_index,
                                    uint32_t input_connection_count,
                                    uint32_t compute_connection_count,
                                    uint32_t max_history_size,
                                    uint32_t variance_percentage,
                                    std::string log_directory,
                                    bool enable_logging) {
  instance_ = new DDLoadBalancerManager(
      scheduler_index, input_connection_count, compute_connection_count,
      max_history_size, variance_percentage, log_directory, enable_logging);
  return instance_;
}

DDLoadBalancerManager* DDLoadBalancerManager::get_instance() {
  assert(instance_ != nullptr);
  return instance_;
}

std::vector<double> DDLoadBalancerManager::get_last_distribution_load() {
  return interval_load_distribution_.get(
      interval_load_distribution_.get_last_key());
}

bool DDLoadBalancerManager::needs_redistribute_load(
    uint64_t interval_index,
    uint64_t speedup_start_interval_index,
    uint64_t speedup_end_interval_index) {
  /*L_(info) << "[needs_redistribute_load] interval_index: " << interval_index
           << " speedup_start_interval_index: " << speedup_start_interval_index
           << " speedup_end_interval_index: " << speedup_end_interval_index;*/
  if (get_load_by_rdma_latency(interval_index, speedup_start_interval_index,
                               speedup_end_interval_index, false)
          .first ||
      get_load_by_ts_processing_duration(interval_index,
                                         speedup_start_interval_index,
                                         speedup_end_interval_index, false)
          .first ||
      get_load_by_ts_completion_duration(interval_index,
                                         speedup_start_interval_index,
                                         speedup_end_interval_index, false)
          .first)
    return true;
  return false;
}

std::vector<double> DDLoadBalancerManager::calculate_new_distribtion_load(
    uint64_t interval_index,
    uint64_t speedup_start_interval_index,
    uint64_t speedup_end_interval_index) {
  std::vector<std::vector<double>> loads;
  // (1) Find load by observed write-RDMA latency from INs
  loads.push_back(get_load_by_rdma_latency(interval_index,
                                           speedup_start_interval_index,
                                           speedup_end_interval_index)
                      .second);
  // (2) Find load by observed msg latency from INs
  /* TODO loads.push_back(get_load_by_msg_latency(interval_index,
    speedup_start_interval_index, speedup_end_interval_index));*/
  // (3) Find load by observed CN blockage durations
  // find_blockgable_compute_node_by_buffer_blockage_time();
  // (4) Find load by observed IN blockage durations
  //
  // (5) Find load by observed processing duration at CNs
  loads.push_back(get_load_by_ts_processing_duration(
                      interval_index, speedup_start_interval_index,
                      speedup_end_interval_index)
                      .second);
  // (6) Find load by observed completion duration at CNs
  loads.push_back(get_load_by_ts_completion_duration(
                      interval_index, speedup_start_interval_index,
                      speedup_end_interval_index)
                      .second);
  // (7) Find load by observed buffer fill-level at CNs
  /*std::vector<double> buffer_fill_level_load = get_load_by_buffer_fill_level(
      speedup_start_interval_index, speedup_end_interval_index);*/
  //
  return calculate_aggregated_average_load_distribution(loads);
}

std::vector<double> DDLoadBalancerManager::consider_new_distribtion_load(
    uint64_t interval_index,
    uint64_t speedup_start_interval_index,
    uint64_t speedup_end_interval_index) {
  std::vector<double> new_load = calculate_new_distribtion_load(
      interval_index, speedup_start_interval_index, speedup_end_interval_index);
  interval_load_distribution_.add(interval_index, new_load);
  return new_load;
}

DDLoadBalancerManager::DDLoadBalancerManager(uint32_t scheduler_index,
                                             uint32_t input_connection_count,
                                             uint32_t compute_connection_count,
                                             uint32_t max_history_size,
                                             uint32_t variance_percentage,
                                             std::string log_directory,
                                             bool enable_logging)
    : scheduler_index_(scheduler_index),
      input_connection_count_(input_connection_count),
      compute_connection_count_(compute_connection_count),
      max_history_size_(max_history_size),
      variance_percentage_(variance_percentage), log_directory_(log_directory),
      enable_logging_(enable_logging) {
  assert(max_history_size_ >= 1);
  compute_interval_data_manager_ = ComputeIntervalDataManager::get_instance();
  interval_load_distribution_.add(
      0, std::vector<double>(compute_connection_count_, 1));
}

////////////////////// Business Logic //////////////////////
/**
 *
 * Prerequisite: Sum of all loads = # of compute nodes
 * ------------- Algo ----------------
 * TODO (1) assert that this call is after a speedup interval
 * (2) Get the median latency[i] of each CN during the speedup phase
 *            Hint: at least one speedup interval is still running!
 * (3) Calculate Statistics (min, median, max, sum) of step#(2)
 * (4) if (max-median)/max <= variance_percentage RETURN
 * (5) Calculate the overload to distribute on other nodes
 * (6) calculate the new load of slowest node
 * (7) distribute the overload equally on all compute nodes to maintain the same
 *     variance
 * TODO For better performance, use weighed load distribution
 *      (closer to median, more share to get)
 *
 */
std::pair<bool, std::vector<double>>
DDLoadBalancerManager::get_load_by_ts_processing_duration(
    uint64_t interval_index,
    uint64_t speedup_start_interval_index,
    uint64_t speedup_end_interval_index,
    bool calculate) {

  std::vector<uint64_t> median_processing_duration =
      retrieve_median_ts_duration(speedup_start_interval_index,
                                  speedup_end_interval_index, true);
  if (true)
    L_(info)
        << "[get_load_by_ts_processing_duration] speedup_start_interval_index: "
        << speedup_start_interval_index
        << " speedup_end_interval_index: " << speedup_end_interval_index
        << " # conns: " << median_processing_duration.size();

  // TODO REMOVE LOGGING PART
  MatrixLog* log = new MatrixLog();
  std::pair<bool, std::vector<double>> load =
      get_load_by_lowering_duration(median_processing_duration, log, calculate);
  if (load.first)
    load_balancing_logs_.add(
        std::pair<uint64_t, MatrixType>(interval_index, MatrixType::TS_PROC),
        log);
  return load;
}

/**
 *
 * Prerequisite: Sum of all loads = # of compute nodes
 * ------------- Algo ----------------
 * TODO (1) assert that this call is after a speedup interval
 * (2) Get the median comp duration[i] of each CN during the speedup phase
 *            Hint: at least one speedup interval is still running!
 * (3) Calculate Statistics (min, median, max, sum) of step#(2)
 * (4) if (max-median)/max <= variance_percentage RETURN
 * (5) Calculate the overload to distribute on other nodes
 * (6) calculate the new load of slowest node
 * (7) distribute the overload equally on all compute nodes to maintain the same
 *     variance
 * TODO For better performance, use weighed load distribution
 *      (closer to median, more share to get)
 *
 */
std::pair<bool, std::vector<double>>
DDLoadBalancerManager::get_load_by_ts_completion_duration(
    uint64_t interval_index,
    uint64_t speedup_start_interval_index,
    uint64_t speedup_end_interval_index,
    bool calculate) {

  std::vector<uint64_t> median_completion_duration =
      retrieve_median_ts_duration(speedup_start_interval_index,
                                  speedup_end_interval_index, false);
  if (true)
    L_(info)
        << "[get_load_by_ts_completion_duration] speedup_start_interval_index: "
        << speedup_start_interval_index
        << " speedup_end_interval_index: " << speedup_end_interval_index
        << " # conns: " << median_completion_duration.size();

  // TODO REMOVE LOGGING PART
  MatrixLog* log = new MatrixLog();
  std::pair<bool, std::vector<double>> load =
      get_load_by_lowering_duration(median_completion_duration, log, calculate);
  if (load.first)
    load_balancing_logs_.add(
        std::pair<uint64_t, MatrixType>(interval_index, MatrixType::TS_COMP),
        log);
  return load;
}

/**
 *
 * Prerequisite: Sum of all loads = # of compute nodes
 * ------------- Algo ----------------
 * TODO (1) assert that this call is after a speedup interval
 * (2) Get the median rdma-write latency[i] of each CN
 *            Hint: at least one speedup interval is still running!
 * (3) Calculate Statistics (min, median, max, sum) of step#(2)
 * (4) if (max-median)/max <= variance_percentage RETURN
 * (5) Calculate the overload to distribute on other nodes
 * (6) calculate the new load of slowest node
 * (7) distribute the overload equally on all compute nodes to maintain the same
 *     variance
 * TODO For better performance, use weighed load distribution
 *      (closer to median, more share to get)
 *
 */
std::pair<bool, std::vector<double>>
DDLoadBalancerManager::get_load_by_rdma_latency(
    uint64_t interval_index,
    uint64_t speedup_start_interval_index,
    uint64_t speedup_end_interval_index,
    bool calculate) {
  std::vector<uint64_t> median_processing_duration =
      retrieve_median_input_latency(speedup_start_interval_index,
                                    speedup_end_interval_index, true);
  if (true)
    L_(info) << "[get_load_by_rdma_latency] speedup_start_interval_index: "
             << speedup_start_interval_index
             << " speedup_end_interval_index: " << speedup_end_interval_index
             << " # conns: " << median_processing_duration.size();
  // TODO REMOVE LOGGING PART
  MatrixLog* log = new MatrixLog();
  std::pair<bool, std::vector<double>> load =
      get_load_by_lowering_duration(median_processing_duration, log, calculate);
  if (load.first)
    load_balancing_logs_.add(std::pair<uint64_t, MatrixType>(
                                 interval_index, MatrixType::RDMA_LATENCY),
                             log);
  return load;
}

/**
 *
 * Prerequisite: Sum of all loads = # of compute nodes
 * ------------- Algo ----------------
 * TODO (1) assert that this call is after a speedup interval
 * (2) Get the median msg latency[i] of each CN
 *            Hint: at least one speedup interval is still running!
 * (3) Calculate Statistics (min, median, max, sum) of step#(2)
 * (4) if (max-median)/max <= variance_percentage RETURN
 * (5) Calculate the overload to distribute on other nodes
 * (6) calculate the new load of slowest node
 * (7) distribute the overload equally on all compute nodes to maintain the same
 *     variance
 * TODO For better performance, use weighed load distribution
 *      (closer to median, more share to get)
 *
 */
std::pair<bool, std::vector<double>>
DDLoadBalancerManager::get_load_by_msg_latency(
    uint64_t interval_index,
    uint64_t speedup_start_interval_index,
    uint64_t speedup_end_interval_index,
    bool calculate) {
  std::vector<uint64_t> median_processing_duration =
      retrieve_median_input_latency(speedup_start_interval_index,
                                    speedup_end_interval_index, false);
  if (true)
    L_(info) << "[get_load_by_msg_latency] speedup_start_interval_index: "
             << speedup_start_interval_index
             << " speedup_end_interval_index: " << speedup_end_interval_index
             << " # conns: " << median_processing_duration.size();

  // TODO REMOVE LOGGING PART
  MatrixLog* log = new MatrixLog();
  std::pair<bool, std::vector<double>> load =
      get_load_by_lowering_duration(median_processing_duration, log, calculate);
  if (load.first)
    load_balancing_logs_.add(std::pair<uint64_t, MatrixType>(
                                 interval_index, MatrixType::MSG_LATENCY),
                             log);
  return load;
}

////////////////////// Common Business Logic //////////////////////
std::pair<bool, std::vector<double>>
DDLoadBalancerManager::get_load_by_lowering_duration(
    std::vector<uint64_t> median_duration, MatrixLog* log, bool calculate) {
  StatisticsCalculator::ListStatistics duration_stats =
      StatisticsCalculator::calculate_list_statistics(median_duration);

  double current_variance =
      ((duration_stats.max_val - duration_stats.median_val) * 1.0) /
      (duration_stats.max_val * 1.0);

  log->stats = duration_stats;
  log->variance = current_variance;
  std::vector<double> load = get_last_distribution_load();
  if (true)
    L_(info) << "[get_load_by_lowering_duration] min duration: "
             << duration_stats.min_val
             << " min index: " << duration_stats.min_indx
             << " max duration: " << duration_stats.max_val
             << " max index: " << duration_stats.max_indx
             << " median: " << duration_stats.median_val << " diff_pecentage "
             << current_variance * 100.0
             << "% max_variance_percentage_: " << variance_percentage_ << "%"
             << ", # of conns: " << median_duration.size()
             << ", prev load conns: " << load.size();
  if (current_variance * 100.0 <= variance_percentage_) {
    return std::pair<bool, std::vector<double>>(false, load);
  }
  if (calculate) {

    double load_to_distribute =
        load[duration_stats.max_indx] * current_variance;
    log->load_to_distribute = load_to_distribute;
    load[duration_stats.max_indx] *= (1.0 - current_variance);
    // TODO For better performance, use weighed load distribution
    // (closer to median, more share to get)
    double sum_new_load = 0.0;
    for (uint32_t i = 0; i < load.size(); i++) {
      // load.size() === sum of all loads
      load[i] += load[i] * load_to_distribute /
                 ((load.size() * 1.0) - load_to_distribute);
      sum_new_load += load[i];
    }
    if (true)
      L_(info) << "[get_load_by_lowering_duration] load_to_distribute: "
               << load_to_distribute << ", sum_new_load: " << sum_new_load
               << ", # of conns: " << load.size() << ", new load "
               << ConstVariables::vector_to_string(load);
    // Add the small fraction to the fastest node to keep the load constant (sum
    // of all loads === # of compute nodes)
    // assert(sum_new_load <= (load.size() * 1.0));
    load[duration_stats.min_indx] += (load.size() * 1.0) - sum_new_load;
  }
  return std::pair<bool, std::vector<double>>(true, load);
}

////////////////////// Helper Methods //////////////////////

std::vector<uint64_t> DDLoadBalancerManager::retrieve_median_ts_duration(
    uint64_t start_interval_index,
    uint64_t end_interval_index,
    bool processing_duration) {
  std::vector<std::vector<uint64_t>> processing_duration_history;

  uint64_t duration;
  // Collect the procession
  for (uint64_t interval = start_interval_index; interval <= end_interval_index;
       interval++) {
    ComputeCompletedIntervalMetaData* meta_data =
        compute_interval_data_manager_->get_actual_interval_meta_data(interval);
    if (processing_duration_history.size() < meta_data->compute_node_count) {
      processing_duration_history.resize(meta_data->compute_node_count);
    }
    for (uint32_t cn_indx = 0; cn_indx < meta_data->compute_node_count;
         ++cn_indx) {
      if (meta_data->compute_statistics[cn_indx] == nullptr)
        continue;
      if (processing_duration)
        duration = meta_data->compute_statistics[cn_indx]
                       ->median_timeslice_processing_duration;
      else
        duration = meta_data->compute_statistics[cn_indx]
                       ->median_timeslice_completion_duration;
      processing_duration_history[cn_indx].push_back(duration);
    }
  }
  return calculate_median_normalization(processing_duration_history);
}

std::vector<uint64_t> DDLoadBalancerManager::retrieve_median_input_latency(
    uint64_t start_interval_index,
    uint64_t end_interval_index,
    bool rdma_write) {
  std::vector<std::vector<uint64_t>> duration_history;

  uint64_t latency;
  // Collect the procession
  for (uint64_t interval = start_interval_index; interval <= end_interval_index;
       interval++) {
    ComputeCompletedIntervalMetaData* meta_data =
        compute_interval_data_manager_->get_actual_interval_meta_data(interval);
    if (duration_history.size() < meta_data->compute_node_count) {
      duration_history.resize(meta_data->compute_node_count);
    }
    for (uint32_t in_indx = 0; in_indx < input_connection_count_; ++in_indx) {
      if (meta_data->input_statistics[in_indx] == nullptr)
        continue;
      for (uint32_t cn_indx = 0; cn_indx < meta_data->compute_node_count;
           ++cn_indx) {
        if (rdma_write)
          latency = meta_data->input_statistics[in_indx]
                        ->median_rdma_latency[cn_indx];
        else
          latency = meta_data->input_statistics[in_indx]
                        ->median_message_latency[cn_indx];
        duration_history[cn_indx].push_back(latency);
      }
    }
  }
  return calculate_median_normalization(duration_history);
}

std::vector<uint64_t> DDLoadBalancerManager::calculate_median_normalization(
    std::vector<std::vector<uint64_t>> duration_history) {
  // Calculate median
  std::vector<uint64_t> median_duration(duration_history.size());
  for (uint32_t i = 0; i < median_duration.size(); ++i) {
    median_duration[i] =
        StatisticsCalculator::get_median_of_list(duration_history[i]);
  }
  return median_duration;
}

std::vector<double>
DDLoadBalancerManager::calculate_aggregated_average_load_distribution(
    std::vector<std::vector<double>> loads) {
  std::vector<double> new_load;
  if (loads.empty())
    return new_load;
  for (uint32_t i = 0; i < loads[0].size(); i++) {
    double sum = 0;
    for (uint32_t j = 0; j < loads.size(); j++)
      sum += loads[j][i];
    new_load.push_back(sum / loads.size());
  }
  return new_load;
}

void DDLoadBalancerManager::generate_log_files() {
  if (!enable_logging_)
    return;

  generate_matrix_stats_logs(MatrixType::RDMA_LATENCY);
  generate_matrix_stats_logs(MatrixType::MSG_LATENCY);
  generate_matrix_stats_logs(MatrixType::TS_COMP);
  generate_matrix_stats_logs(MatrixType::TS_PROC);

  std::ofstream log_file;
  log_file.open(log_directory_ + "/" + std::to_string(scheduler_index_) +
                ".compute.interval_load_info.out");

  log_file << std::setw(25) << "Interval";

  for (uint32_t i = 0; i < compute_connection_count_; i++)
    log_file << std::setw(25) << "CN[" << i << "]";
  log_file << "\n";

  for (SizedMap<uint64_t, std::vector<double>>::iterator it =
           interval_load_distribution_.get_begin_iterator();
       it != interval_load_distribution_.get_end_iterator(); ++it) {
    log_file << std::setw(25) << it->first;
    for (uint32_t i = 0; i < it->second.size(); i++)
      log_file << std::setw(25) << it->second[i];
    log_file << "\n";
  }

  log_file.flush();
  log_file.close();
}

void DDLoadBalancerManager::generate_matrix_stats_logs(MatrixType type) {

  std::string type_str = "";
  switch (type) {
  case RDMA_LATENCY:
    type_str = "RDMA_LATENCY";
    break;
  case MSG_LATENCY:
    type_str = "MSG_LATENCY";
    break;
  case TS_COMP:
    type_str = "TS_COMP";
    break;
  case TS_PROC:
    type_str = "TS_PROC";
    break;
  }
  uint64_t max_interval_index = load_balancing_logs_.get_last_key().first;
  MatrixLog* log;

  std::ofstream log_file;
  log_file.open(log_directory_ + "/" + std::to_string(scheduler_index_) +
                ".compute.interval_load_info." + type_str + ".out");

  log_file << std::setw(25) << "Interval";
  // RDMA_LATENCY CALC STATS
  log_file << std::setw(25) << "min_ind" << std::setw(25) << "min_val"
           << std::setw(25) << "max_ind" << std::setw(25) << "max_val"
           << std::setw(25) << "med_val" << std::setw(25) << "variance"
           << std::setw(25) << "load_diff"
           << "\n";
  for (uint64_t i = 0; i <= max_interval_index; i++) {

    if (!load_balancing_logs_.contains(
            std::pair<uint64_t, MatrixType>(i, type)))
      continue;
    log = load_balancing_logs_.get(std::pair<uint64_t, MatrixType>(i, type));
    log_file << std::setw(25) << i << std::setw(25) << log->stats.min_indx
             << std::setw(25) << log->stats.min_val << std::setw(25)
             << log->stats.max_indx << std::setw(25) << log->stats.max_val
             << std::setw(25) << log->stats.median_val << std::setw(25)
             << log->variance << std::setw(25) << log->load_to_distribute
             << "\n";
  }

  log_file.flush();
  log_file.close();
}
////////////////////// TO BE UPDATED //////////////////////

/*
uint32_t
DDLoadBalancerManager::find_blockgable_compute_node_by_buffer_blockage_time()
{
  // +1 because last interval is not complete yet
  if (compute_interval_data_manager_->get_actual_interval_meta_data_size() + 1
< variance_history_size_) return ConstVariables::MINUS_ONE;

  std::vector<uint64_t> sum_IB_blockage_time, sum_CB_blockage_time;

  // calculate sum IB/CB blockage time of each input node over a
  // variance_history_size_ of intervals
  uint32_t count = 0;
  SizedMap<uint64_t, ComputeCompletedIntervalMetaData*>::iterator
      end_it = compute_interval_data_manager_
                   ->get_actual_interval_meta_data_end_interator(),
      begin_it = compute_interval_data_manager_
                     ->get_actual_interval_meta_data_begin_interator();
  --end_it;
  while (--end_it != begin_it && count++ < variance_history_size_) {
    ComputeCompletedIntervalMetaData* meta_data = end_it->second;
    if (count == 0 ||
        sum_IB_blockage_time.size() < meta_data->compute_node_count) {
      sum_IB_blockage_time.resize(meta_data->compute_node_count, 0);
      sum_CB_blockage_time.resize(meta_data->compute_node_count, 0);
    }
    calculate_average_blockage_time_of_compute_nodes(
        meta_data, sum_IB_blockage_time, true);
    calculate_average_blockage_time_of_compute_nodes(
        meta_data, sum_CB_blockage_time, false);
  }
  // find slowest
  StatisticsCalculator::ListStatistics
      IB_blockage_stats =
          StatisticsCalculator::calculate_list_statistics(sum_IB_blockage_time),
      CB_blockage_stats =
          StatisticsCalculator::calculate_list_statistics(sum_CB_blockage_time);
  double IB_least_diff_pecentage =
      IB_blockage_stats.min_val > 0
          ? (((IB_blockage_stats.median_val - IB_blockage_stats.min_val) *
              1.0) /
             (IB_blockage_stats.min_val * 1.0)) *
                100.0
          : 0;
  // TODO CB_least_diff_pecentage =
//(CB_blockage_stats.min_val / CB_blockage_stats.median_val) * 100.0

  if (true)
    L_(info) << "[find_blockgable_compute_node_by_buffer_blockage_time] last "
                "actual interval: "
             <<
compute_interval_data_manager_->get_last_actual_interval_index()
             << " variance_history_size_: " << variance_history_size_
             << " min IB Blockage: "
             << (IB_blockage_stats.min_val / variance_history_size_)
             << " min index: " << IB_blockage_stats.min_indx
             << " max IB Blockge: "
             << (IB_blockage_stats.max_val / variance_history_size_)
             << " max index: " << IB_blockage_stats.max_indx << " median: "
             << (IB_blockage_stats.median_val / variance_history_size_)
             << " IB_least_diff_pecentage " << IB_least_diff_pecentage
             << "% max_variance_percentage_: " << variance_percentage_ << "%";
  if (IB_least_diff_pecentage > variance_percentage_) {
    return IB_blockage_stats.min_indx;
  }
  return ConstVariables::MINUS_ONE;
}

uint32_t DDLoadBalancerManager::
    get_slowest_input_node_of_slow_compute_node_using_blockage_time(
        uint32_t compute_index) {
  if (compute_interval_data_manager_->get_actual_interval_meta_data_size() + 2
< variance_history_size_) return ConstVariables::MINUS_ONE;

  std::vector<uint64_t> sum_IB_blockage_time(input_connection_count_, 0);
  // calculate sum latencies of the input node with different compute nodes
  // over a variance_history_size_ of intervals
  uint32_t count = 0;
  SizedMap<uint64_t, ComputeCompletedIntervalMetaData*>::iterator
      end_it = compute_interval_data_manager_
                   ->get_actual_interval_meta_data_end_interator(),
      begin_it = compute_interval_data_manager_
                     ->get_actual_interval_meta_data_begin_interator();
  do {
    --end_it;
    ComputeCompletedIntervalMetaData* meta_data = end_it->second;

    calculate_sum_blockage_time_of_input_nodes(meta_data,
sum_IB_blockage_time, compute_index, true);
    ++count;
  } while (end_it != begin_it && count < variance_history_size_);
  // find slowest
  StatisticsCalculator::ListStatistics IB_blockage_history_stats =
      StatisticsCalculator::calculate_list_statistics(sum_IB_blockage_time);
  double IB_blockage_least_diff_pecentage =
      IB_blockage_history_stats.min_val > 0
          ? (((IB_blockage_history_stats.median_val -
               IB_blockage_history_stats.min_val) *
              1.0) /
             (IB_blockage_history_stats.min_val * 1.0)) *
                100.0
          : 0;

  if (true)
    L_(info) << "[get_slowest_input_node_of_slow_compute_node_using_blockage_"
                "time] last actual "
                "interval: "
             <<
compute_interval_data_manager_->get_last_actual_interval_index()
             << " variance_history_size_: " << variance_history_size_
             << " compute index: " << compute_index << " min IB blockage time:
"
             << (IB_blockage_history_stats.min_val / variance_history_size_)
             << " min input index: " << IB_blockage_history_stats.min_indx
             << " max IB blockage time: "
             << (IB_blockage_history_stats.max_val / variance_history_size_)
             << " max compute index: " << IB_blockage_history_stats.max_indx
             << " median: "
             << (IB_blockage_history_stats.median_val /
variance_history_size_)
             << " IB Least diff_pecentage " <<
IB_blockage_least_diff_pecentage
             << "% max_variance_percentage_: " << variance_percentage_ << "%";
  if (IB_blockage_least_diff_pecentage > variance_percentage_) {
    return IB_blockage_history_stats.min_indx;
  }
  return ConstVariables::MINUS_ONE;
}

/////// Helper methods

// Calculate averaged IB/CB blockage time of intervals of compute nodes
void DDLoadBalancerManager::calculate_average_blockage_time_of_compute_nodes(
    ComputeCompletedIntervalMetaData* meta_data,
    std::vector<uint64_t>& blockage_time_to_add,
    bool IB_blockage) {
  for (uint32_t cn_indx = 0; cn_indx < meta_data->compute_node_count;
       ++cn_indx) {
    blockage_time_to_add[cn_indx] =
        calculate_average_blockage_time_of_compute_index(meta_data, cn_indx,
                                                         IB_blockage);
  }
}

//
uint64_t
DDLoadBalancerManager::calculate_average_blockage_time_of_compute_index(
    ComputeCompletedIntervalMetaData* meta_data,
    uint32_t compute_indx,
    bool IB_blockage) {
  uint64_t blockage_time_sum = 0;
  for (uint32_t in_indx = 0; in_indx < input_connection_count_; ++in_indx) {
    if (IB_blockage)
      blockage_time_sum += meta_data->input_statistics[in_indx]
                               ->sum_IB_blockage_durations[compute_indx];
    else
      blockage_time_sum += meta_data->input_statistics[in_indx]
                               ->sum_CB_blockage_durations[compute_indx];
  }
  return (blockage_time_sum / input_connection_count_);
}

void DDLoadBalancerManager::calculate_sum_blockage_time_of_input_nodes(
    ComputeCompletedIntervalMetaData* meta_data,
    std::vector<uint64_t>& sum_blockage_time_to_add,
    uint32_t compute_indx,
    bool IB_blockage) {
  for (uint32_t in_indx = 0; in_indx < input_connection_count_; ++in_indx) {
    if (IB_blockage)
      sum_blockage_time_to_add[in_indx] +=
          meta_data->input_statistics[in_indx]
              ->sum_IB_blockage_durations[compute_indx];
    else
      sum_blockage_time_to_add[in_indx] +=
          meta_data->input_statistics[in_indx]
              ->sum_CB_blockage_durations[compute_indx];
  }
}
*/
DDLoadBalancerManager* DDLoadBalancerManager::instance_ = nullptr;
} // namespace tl_libfabric
