// Copyright 2020 Farouk Salem <salem@zib.de>

#include "IntervalEventDurationLogger.hpp"

#include <stdint.h>

namespace tl_libfabric {

IntervalEventDurationLogger::IntervalEventDurationLogger(
    uint32_t destination_count)
    : destination_count_(destination_count) {}

IntervalEventDurationLogger::~IntervalEventDurationLogger() {

  while (!pending_events_.empty()) {
    pending_events_.remove(pending_events_.get_last_key());
  }

  while (!calculated_interval_duration_.empty()) {
    calculated_interval_duration_.remove(
        calculated_interval_duration_.get_last_key());
  }
}

void IntervalEventDurationLogger::log_event_start(/*uint64_t interval_index,*/
                                                  uint32_t destination_index,
                                                  uint64_t event_id) {
  if (last_recieved_event_id_ < event_id)
    last_recieved_event_id_ = event_id;

  if (pending_events_.contains(event_id)) {
    if (pending_events_.get(event_id).first == destination_index)
      return;
    // Event is redistributed to another destination
    pending_events_.remove(event_id);
  }

  pending_events_.add(
      event_id,
      std::pair<uint32_t, std::chrono::high_resolution_clock::time_point>(
          destination_index, std::chrono::high_resolution_clock::now()));
}

void IntervalEventDurationLogger::log_event_completion(
    uint64_t interval_index, uint32_t destination_index, uint64_t event_id) {

  if (!pending_events_.contains(event_id))
    return;
  if (pending_events_.get(event_id).first != destination_index) {
    L_(error) << "Event " << event_id
              << "sent to two different destinations: " << destination_index
              << " and " << pending_events_.get(event_id).first;
  }

  int64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::high_resolution_clock::now() -
                         pending_events_.get(event_id).second)
                         .count();
  assert(duration >= 0);
  process_calculated_duration(interval_index, destination_index, duration);

  // clean
  assert(pending_events_.remove(event_id));
}

std::vector<uint64_t>
IntervalEventDurationLogger::retrieve_calculated_events_duration(
    uint64_t interval_index) {
  trigger_interval_completion(interval_index);
  if (!calculated_interval_duration_.contains(interval_index))
    return std::vector<uint64_t>();

  std::vector<uint64_t>* calculated_durations =
      calculated_interval_duration_.get(interval_index);
  return *calculated_durations;
}

uint64_t IntervalEventDurationLogger::get_calculated_events_duration(
    uint64_t interval_index, uint32_t destination_index) {

  trigger_interval_completion(interval_index);
  if (!calculated_interval_duration_.contains(interval_index))
    return ConstVariables::ZERO;

  std::vector<uint64_t>* calculated_durations =
      calculated_interval_duration_.get(interval_index);
  if (calculated_durations->size() <= destination_index)
    return ConstVariables::ZERO;

  return (*calculated_durations)[destination_index];
}

uint64_t IntervalEventDurationLogger::get_next_event_id() {
  if (last_recieved_event_id_ == UINT64_MAX) {
    last_recieved_event_id_ = 0;
  }
  return last_recieved_event_id_ + 1;
}

} // namespace tl_libfabric
