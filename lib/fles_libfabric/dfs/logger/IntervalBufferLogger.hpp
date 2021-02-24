// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "SizedMap.hpp"
#include "log.hpp"

#include <list>
#include <vector>

namespace tl_libfabric {
/**
 *
 */
class IntervalBufferLogger {
public:
  /**
   * Log the buffer fill level of all nodes at a given time. returns false if
   * the interval statistics are already calculated
   */
  bool log_buffer_level(uint64_t interval_index,
                        std::vector<uint32_t> individual_levels,
                        uint64_t time = ConstVariables::MINUS_ONE);

  /**
   * Retrieve the median fill level of all buffers for a particular interval and
   * it automatically triggers end of an interval
   *
   */
  std::vector<uint64_t> retrieve_median_levels(uint64_t interval_index);

  /**
   * Retrieve the median fill level a particular interval and buffer index.
   * It automatically triggers end of an interval
   *
   */
  uint64_t get_median_level(uint64_t interval_index, uint32_t buffer_index);

private:
  struct LogEntry {
    uint32_t buffer_index;
    uint32_t buffer_level;
    uint64_t log_time;
    LogEntry(uint32_t ind, uint32_t level, uint64_t time)
        : buffer_index(ind), buffer_level(level), log_time(time) {}
  };

  /**
   * Calculate the median level of different buffers for a particular interval
   */
  void calculate_median_level(uint64_t interval_index);

  /**
   * Retrieve the log entries of an itnerval
   */
  std::list<LogEntry*>* retrieve_interval_entries(uint64_t interval_index);

  // <interval, list of buffer level entries>
  SizedMap<uint64_t, std::list<LogEntry*>*> timer_logger_;

  // <interval, <buffer_index, median latency>>
  SizedMap<uint64_t, std::vector<uint64_t>> median_level_;
};
} // namespace tl_libfabric
