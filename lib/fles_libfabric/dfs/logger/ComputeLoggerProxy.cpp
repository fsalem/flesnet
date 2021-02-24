// Copyright 2020 Farouk Salem <salem@zib.de>

#include "ComputeLoggerProxy.hpp"

namespace tl_libfabric {

ComputeLoggerProxy::ComputeLoggerProxy(uint32_t destination_count) {
  timeslice_completion_logger_ = new IntervalEventDurationMedianLogger(1);
  timeslice_processing_logger_ = new IntervalEventDurationMedianLogger(1);
  timeslice_filling_logger_ =
      new IntervalEventDurationMedianLogger(destination_count);
}

ComputeLoggerProxy* ComputeLoggerProxy::get_instance() {
  assert(instance_ != nullptr);
  return instance_;
}

ComputeLoggerProxy*
ComputeLoggerProxy::init_instance(uint32_t destination_count) {
  if (instance_ == nullptr) {
    instance_ = new ComputeLoggerProxy(destination_count);
  }
  return instance_;
}

//// Buffer Level Methods
void ComputeLoggerProxy::log_buffer_level(
    uint64_t interval_index, std::vector<uint64_t> individual_levels) {
  for (uint32_t i = 0; i < individual_levels.size(); i++) {
    timeslice_filling_logger_->process_calculated_duration(
        interval_index, i, individual_levels[i]);
  }
}

std::vector<uint64_t>
ComputeLoggerProxy::retrieve_median_buffer_levels(uint64_t interval_index) {
  return timeslice_filling_logger_->retrieve_calculated_events_duration(
      interval_index);
}

//////

IntervalEventDurationMedianLogger*
ComputeLoggerProxy::get_timeslice_operation_object(enum TS_OPEATION operation) {
  switch (operation) {
  case COMPLETING:
    return timeslice_completion_logger_;
  case PROCESSING:
    return timeslice_processing_logger_;
  case FILLING:
    return timeslice_filling_logger_;
  }
  return nullptr;
}
void ComputeLoggerProxy::log_start_timeslice_operation(
    enum TS_OPEATION operation, /*uint64_t interval_index,*/
    uint32_t destination_index,
    uint64_t timeslice) {
  assert(operation != FILLING);
  get_timeslice_operation_object(operation)->log_event_start(destination_index,
                                                             timeslice);
}

void ComputeLoggerProxy::log_timeslice_operation_completion(
    enum TS_OPEATION operation,
    uint64_t interval_index,
    uint32_t destination_index,
    uint64_t timeslice) {
  assert(operation != FILLING);
  get_timeslice_operation_object(operation)->log_event_completion(
      interval_index, destination_index, timeslice);
}

std::vector<uint64_t>
ComputeLoggerProxy::retrieve_timeslice_operation_median_duration(
    enum TS_OPEATION operation, uint64_t interval_index) {
  return get_timeslice_operation_object(operation)
      ->retrieve_calculated_events_duration(interval_index);
}

uint64_t ComputeLoggerProxy::get_timeslice_operation_median_duration(
    enum TS_OPEATION operation,
    uint64_t interval_index,
    uint32_t destination_index) {
  return get_timeslice_operation_object(operation)
      ->get_calculated_events_duration(interval_index, destination_index);
}

//// Variables
ComputeLoggerProxy* ComputeLoggerProxy::instance_ = nullptr;

} // namespace tl_libfabric
