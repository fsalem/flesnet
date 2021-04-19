// Copyright 2020 Farouk Salem <salem@zib.de>

#include "IntervalBufferLogger.hpp"

namespace tl_libfabric {

std::list<IntervalBufferLogger::LogEntry*>*
IntervalBufferLogger::retrieve_interval_entries(uint64_t interval_index) {
  if (!timer_logger_.contains(interval_index)) {
    timer_logger_.add(interval_index, new std::list<LogEntry*>());
  }
  return timer_logger_.get(interval_index);
}

bool IntervalBufferLogger::log_buffer_level(
    uint64_t interval_index,
    std::vector<uint32_t> individual_levels,
    uint64_t time) {

  if (median_level_.contains(interval_index)) {
    return false;
  }

  std::list<LogEntry*>* entries = retrieve_interval_entries(interval_index);

  for (uint32_t ind = 0; ind < individual_levels.size(); ++ind) {
    entries->push_back(new LogEntry(ind, individual_levels[ind], time));
  }

  return true;
}

std::vector<uint64_t>
IntervalBufferLogger::retrieve_median_levels(uint64_t interval_index) {
  if (!median_level_.contains(interval_index))
    calculate_median_level(interval_index);

  assert(median_level_.contains(interval_index));
  return median_level_.get(interval_index);
}

uint64_t IntervalBufferLogger::get_median_level(uint64_t interval_index,
                                                uint32_t buffer_index) {
  if (!median_level_.contains(interval_index))
    calculate_median_level(interval_index);

  assert(median_level_.contains(interval_index));
  std::vector<uint64_t> median_levels = median_level_.get(interval_index);
  assert(median_levels.size() > buffer_index);
  return median_levels[buffer_index];
}

void IntervalBufferLogger::calculate_median_level(uint64_t interval_index) {

  if (median_level_.contains(interval_index))
    return;
  // <buffer_index, <levels>>
  SizedMap<uint32_t, std::vector<uint64_t>> buffer_levels;
  std::list<LogEntry*>* entries = retrieve_interval_entries(interval_index);
  std::list<LogEntry*>::iterator it = entries->begin();

  while (it != entries->end()) {
    if (!buffer_levels.contains((*it)->buffer_index)) {
      buffer_levels.add((*it)->buffer_index, std::vector<uint64_t>());
    }
    buffer_levels.get((*it)->buffer_index).push_back((*it)->buffer_level);
    ++it;
  }

  std::vector<uint64_t> medians(buffer_levels.get_last_key(),
                                ConstVariables::MINUS_ONE);
  SizedMap<uint32_t, std::vector<uint64_t>>::iterator buffer_it =
      buffer_levels.get_begin_iterator();
  while (buffer_it != buffer_levels.get_end_iterator()) {
    std::sort(buffer_it->second.begin(), buffer_it->second.end());
    medians[buffer_it->first] =
        buffer_it->second[(buffer_it->second.size() / 2)];
    ++buffer_it;
  }

  median_level_.add(interval_index, medians);
  // clean
  timer_logger_.remove(interval_index);
  delete entries;
}

} // namespace tl_libfabric
