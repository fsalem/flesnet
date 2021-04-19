// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "SizedMap.hpp"
#include "log.hpp"

#include <chrono>
#include <list>
#include <set>
#include <vector>

namespace tl_libfabric {
/**
 *
 */
class IntervalEventDurationLogger {
public:
  IntervalEventDurationLogger(uint32_t destination_count);

  virtual ~IntervalEventDurationLogger();
  /**
   * Log the time when an event is started
   */
  void log_event_start(/*uint64_t interval_index,*/
                       uint32_t destination_index,
                       uint64_t event_id);

  /**
   * Log the time when an event is ended
   */
  void log_event_completion(uint64_t interval_index,
                            uint32_t destination_index,
                            uint64_t event_id);

  /**
   * Retrieve the calculated duration for a particular interval and
   * it automatically triggers end of an interval
   *
   */
  std::vector<uint64_t>
  retrieve_calculated_events_duration(uint64_t interval_index);

  /**
   * Get the calculated duration of a particular interval and destination index.
   * It automatically triggers end of an interval
   *
   */
  uint64_t get_calculated_events_duration(uint64_t interval_index,
                                          uint32_t destination_index);

  /**
   *
   */
  uint64_t get_next_event_id();

protected:
  /**
   *
   */
  virtual void process_calculated_duration(uint64_t interval_index,
                                           uint32_t destination_index,
                                           uint64_t duration) = 0;

  /**
   *
   */
  virtual void trigger_interval_completion(uint64_t interval_index) = 0;
  //
  //<timeslice, <dest_index, start time>>
  SizedMap<uint64_t,
           std::pair<uint32_t, std::chrono::high_resolution_clock::time_point>>
      pending_events_;

  uint32_t destination_count_;

  // <interval, <buffer_index, calculated value>>
  SizedMap<uint64_t, std::vector<uint64_t>*> calculated_interval_duration_;

  uint64_t last_recieved_event_id_ = 0;
};
} // namespace tl_libfabric
