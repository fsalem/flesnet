// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "SizedMap.hpp"
#include "dfs/model/interval_manager/ComputeCompletedIntervalMetaData.hpp"
#include "dfs/model/interval_manager/ComputeProposedIntervalMetaData.hpp"
#include "dfs/model/interval_manager/InputIntervalMetaData.hpp"

#include <fstream>
#include <iomanip>

namespace tl_libfabric {
/**
 * Singleton Compute Data Manager that stores all calculated Data
 */
class ComputeIntervalDataManager {
public:
  // Initialize the instance and retrieve it
  static ComputeIntervalDataManager* get_instance(uint32_t scheduler_index,
                                                  uint64_t max_history_size,
                                                  std::string log_directory,
                                                  bool enable_logging);

  // Get singleton instance
  static ComputeIntervalDataManager* get_instance();

  ////////////////// timeslice_completion_duration
  bool add_timeslice_completion_duration(uint64_t timeslice, double duration);

  //
  double get_timeslice_completion_duration(uint64_t timeslice);

  //
  bool contain_timeslice_completion_duration(uint64_t timeslice);

  //
  bool remove_timeslice_completion_duration(uint64_t timeslice);

  ////////////////// timeslice_timed_out_duration
  bool add_timeslice_timed_out_duration(uint64_t timeslice, double duration);

  //
  double get_timeslice_timed_out_duration(uint64_t timeslice);

  //
  bool contain_timeslice_timed_out_duration(uint64_t timeslice);

  //
  bool remove_timeslice_timed_out_duration(uint64_t timeslice);

  ////////////////// proposed_interval_meta_data
  bool
  add_proposed_interval_meta_data(uint64_t interval,
                                  ComputeProposedIntervalMetaData* meta_data);

  //
  ComputeProposedIntervalMetaData*
  get_proposed_interval_meta_data(uint64_t interval);

  //
  bool contain_proposed_interval_meta_data(uint64_t interval);

  //
  bool remove_proposed_interval_meta_data(uint64_t interval);

  ////////////////// actual_interval_meta_data_
  bool
  add_actual_interval_meta_data(uint64_t interval,
                                ComputeCompletedIntervalMetaData* meta_data);

  //
  ComputeCompletedIntervalMetaData*
  get_actual_interval_meta_data(uint64_t interval);

  //
  uint64_t get_last_actual_interval_index();

  //
  uint64_t get_actual_interval_meta_data_size();

  //
  bool contain_actual_interval_meta_data(uint64_t interval);

  //
  bool remove_actual_interval_meta_data(uint64_t interval);

  //
  bool is_actual_interval_meta_data_empty();

  //
  SizedMap<uint64_t, ComputeCompletedIntervalMetaData*>::iterator
  get_actual_interval_meta_data_begin_interator();

  //
  SizedMap<uint64_t, ComputeCompletedIntervalMetaData*>::iterator
  get_actual_interval_meta_data_end_interator();

  //////// Generate log files of the stored data
  void generate_log_files();

private:
  ComputeIntervalDataManager(uint32_t scheduler_index,
                             uint64_t max_history_size,
                             std::string log_directory,
                             bool enable_logging);

  // The singleton instance for this class
  static ComputeIntervalDataManager* instance_;

  // Compute process index
  uint32_t scheduler_index_;

  // The log directory
  std::string log_directory_;

  // Max arrays/maps/sets size
  uint64_t max_history_size_;

  bool enable_logging_;

  // Time to complete each timeslice
  SizedMap<uint64_t, double> timeslice_completion_duration_;

  // The timed out Timeslices <Timeslice_id, duration since first arrival>
  SizedMap<uint64_t, double> timeslice_timed_out_;

  // Proposed interval meta-data
  SizedMap<uint64_t, ComputeProposedIntervalMetaData*>
      proposed_interval_meta_data_;

  // Unified actual interval meta-data
  // TODO ... this one is a mix of Compute&Input!
  SizedMap<uint64_t, ComputeCompletedIntervalMetaData*>
      actual_interval_meta_data_;
};
} // namespace tl_libfabric
