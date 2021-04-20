// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "IntervalBufferLogger.hpp"
#include "IntervalEventDurationMedianLogger.hpp"
#include "log.hpp"

namespace tl_libfabric {
/**
 *  Facade layer of Logger functions
 */
class ComputeLoggerProxy {
public:
  enum TS_OPEATION { COMPLETING, PROCESSING, FILLING };

  static ComputeLoggerProxy* get_instance();

  static ComputeLoggerProxy* init_instance(uint64_t scheduler_index,
                                           uint32_t destination_count,
                                           std::string log_directory,
                                           bool enable_logging);

  ///////////////// BUFFER LEVEL METHODS/////////////////
  /**
   * Log the buffer fill level of all nodes at a given time. returns false if
   * the interval statistics are already calculated
   */
  void log_buffer_level(uint64_t interval_index,
                        std::vector<uint64_t> individual_levels);

  /**
   * Retrieve the median fill level of all buffers for a particular interval and
   * it automatically triggers end of an interval
   *
   */
  std::vector<uint64_t> retrieve_median_buffer_levels(uint64_t interval_index);

  ///////////////// Latency METHODS/////////////////
  /**
   * Log the time when the first contribution of a timeslice is arrived or the
   * time when a timeslice is being processed
   */
  void log_start_timeslice_operation(
      enum TS_OPEATION operation, /*uint64_t interval_index,*/
      uint32_t destination_index,
      uint64_t timeslice);

  /**
   * Log the time when a timeslice is completed or processed
   */
  void log_timeslice_operation_completion(enum TS_OPEATION operation,
                                          uint64_t interval_index,
                                          uint32_t destination_index,
                                          uint64_t timeslice);

  /**
   * Retrieve the median duration for a particular interval and
   * it automatically triggers end of an interval
   *
   */
  std::vector<uint64_t>
  retrieve_timeslice_operation_median_duration(enum TS_OPEATION operation,
                                               uint64_t interval_index);

  /**
   * Retrieve the median duration of a particular interval and destination
   * index. It automatically triggers end of an interval
   *
   */
  uint64_t get_timeslice_operation_median_duration(enum TS_OPEATION operation,
                                                   uint64_t interval_index,
                                                   uint32_t destination_index);

  /**
   *
   */
  void generate_log_files();

private:
  ComputeLoggerProxy(uint64_t scheduler_index,
                     uint32_t destination_count,
                     std::string log_directory,
                     bool enable_logging);

  /**
   *
   */
  IntervalEventDurationMedianLogger*
  get_timeslice_operation_object(enum TS_OPEATION operation);
  //// Variables
  IntervalEventDurationMedianLogger* timeslice_completion_logger_;
  IntervalEventDurationMedianLogger* timeslice_processing_logger_;
  IntervalEventDurationMedianLogger* timeslice_filling_logger_;

  static ComputeLoggerProxy* instance_;
};
} // namespace tl_libfabric
