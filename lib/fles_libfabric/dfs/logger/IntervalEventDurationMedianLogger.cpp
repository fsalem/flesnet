// Copyright 2020 Farouk Salem <salem@zib.de>

#include "IntervalEventDurationMedianLogger.hpp"

namespace tl_libfabric {

IntervalEventDurationMedianLogger::IntervalEventDurationMedianLogger(
    uint32_t destination_count)
    : IntervalEventDurationLogger(destination_count) {}

void IntervalEventDurationMedianLogger::process_calculated_duration(
    uint64_t interval_index, uint32_t destination_index, uint64_t duration) {

  SizedMap<uint64_t, std::vector<uint64_t>*>* destination_durations;

  if (pending_interval_duration_.contains(interval_index)) {
    destination_durations = pending_interval_duration_.get(interval_index);
  } else {
    destination_durations = new SizedMap<uint64_t, std::vector<uint64_t>*>();
    pending_interval_duration_.add(interval_index, destination_durations);
  }

  std::vector<uint64_t>* durations;
  if (destination_durations->contains(destination_index)) {
    durations = destination_durations->get(destination_index);
  } else {
    durations = new std::vector<uint64_t>();
    destination_durations->add(destination_index, durations);
  }
  durations->push_back(duration);
}

void IntervalEventDurationMedianLogger::trigger_interval_completion(
    uint64_t interval_index) {

  if (calculated_interval_duration_.contains(interval_index))
    return;

  SizedMap<uint64_t, std::vector<uint64_t>*>* destination_durations;
  if (!pending_interval_duration_.contains(interval_index)) {
    return;
  } else {
    destination_durations = pending_interval_duration_.get(interval_index);
  }

  std::vector<uint64_t>* medians =
      new std::vector<uint64_t>(destination_count_, 0);

  // iterate on the sizedMap
  for (uint32_t dest = 0; dest < destination_count_; dest++) {
    if (!destination_durations->contains(dest))
      continue;
    std::vector<uint64_t>* durations = destination_durations->get(dest);
    std::sort(durations->begin(), durations->end());
    (*medians)[dest] = (*durations)[(durations->size()) / 2];
    // clean
    delete durations;
  }

  calculated_interval_duration_.add(interval_index, medians);
  // clean
  pending_interval_duration_.remove(interval_index);
  delete destination_durations;
}
} // namespace tl_libfabric
