// Copyright 2020 Farouk Salem <salem@zib.de>

#include "IntervalEventDurationSumLogger.hpp"

namespace tl_libfabric {

IntervalEventDurationSumLogger::IntervalEventDurationSumLogger(
    uint32_t destination_count)
    : IntervalEventDurationLogger(destination_count) {}

void IntervalEventDurationSumLogger::process_calculated_duration(
    uint64_t interval_index, uint32_t destination_index, uint64_t duration) {
  std::vector<uint64_t>* sum;
  if (calculated_interval_duration_.contains(interval_index)) {
    sum = calculated_interval_duration_.get(interval_index);
  } else {
    sum = new std::vector<uint64_t>(destination_count_, 0);
    calculated_interval_duration_.add(interval_index, sum);
  }
  (*sum)[destination_index] += duration;
}

void IntervalEventDurationSumLogger::trigger_interval_completion(
    uint64_t /*interval_index*/) {
  // For calculating the sum, nothing is needed to be done here
}
} // namespace tl_libfabric
