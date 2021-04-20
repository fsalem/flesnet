// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "IntervalEventDurationLogger.hpp"
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
class IntervalEventDurationMedianLogger : public IntervalEventDurationLogger {
public:
  IntervalEventDurationMedianLogger(uint64_t scheduler_index,
                                    uint32_t destination_count,
                                    std::string log_key,
                                    std::string log_directory,
                                    bool enable_logging);
  /**
   * This method adds the calculated duration to the corresponding value of the
   * destination_index as pending to calculate the median at the end
   */
  void process_calculated_duration(uint64_t interval_index,
                                   uint32_t destination_index,
                                   uint64_t duration) override;

private:
  /**
   * It triggers the end of an interval. It calculates the median of an interval
   */
  void trigger_interval_completion(uint64_t interval_index) override;

  // <interval, <buffer_index, durations>>
  SizedMap<uint64_t, SizedMap<uint64_t, std::vector<uint64_t>*>*>
      pending_interval_duration_;
};
} // namespace tl_libfabric
