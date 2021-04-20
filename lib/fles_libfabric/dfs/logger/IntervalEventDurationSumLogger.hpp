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
class IntervalEventDurationSumLogger : public IntervalEventDurationLogger {
public:
  IntervalEventDurationSumLogger(uint64_t scheduler_index,
                                 uint32_t destination_count,
                                 std::string log_key,
                                 std::string log_directory,
                                 bool enable_logging);

private:
  /**
   * This method adds the calculated duration to the corresponding value of the
   * destination_index
   */
  void process_calculated_duration(uint64_t interval_index,
                                   uint32_t destination_index,
                                   uint64_t duration) override;

  /**
   * It triggers the end of an interval. In case of sum, it is useless because
   * sum is already calculated while processing the calculated duration
   */
  void trigger_interval_completion(uint64_t interval_index) override;

  //
};
} // namespace tl_libfabric
