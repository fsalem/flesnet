// Copyright 2020 Farouk Salem <salem@zib.de>

#include "InputLoggerProxy.hpp"

namespace tl_libfabric {

InputLoggerProxy::InputLoggerProxy(uint64_t scheduler_index,
                                   uint32_t destination_count,
                                   std::string log_directory,
                                   bool enable_logging) {
  local_blockage_logger_ = new IntervalEventDurationSumLogger(
      scheduler_index, destination_count, "input.local_blockage", log_directory,
      enable_logging);
  remote_blockage_logger_ = new IntervalEventDurationSumLogger(
      scheduler_index, destination_count, "input.remote_blockage",
      log_directory, enable_logging);
  timeslice_message_latency_ = new IntervalEventDurationMedianLogger(
      scheduler_index, destination_count, "input.msg_latency", log_directory,
      enable_logging);
  timeslice_write_latency_ = new IntervalEventDurationMedianLogger(
      scheduler_index, destination_count, "input.rdma_latency", log_directory,
      enable_logging);
}

InputLoggerProxy* InputLoggerProxy::get_instance() {
  assert(instance_ != nullptr);
  return instance_;
}

InputLoggerProxy* InputLoggerProxy::init_instance(uint64_t scheduler_index,
                                                  uint32_t destination_count,
                                                  std::string log_directory,
                                                  bool enable_logging) {
  if (instance_ == nullptr) {
    instance_ = new InputLoggerProxy(scheduler_index, destination_count,
                                     log_directory, enable_logging);
  }
  return instance_;
}

//// Blockage methods
IntervalEventDurationSumLogger*
InputLoggerProxy::get_blockage_object(enum LOG_TYPE blockage_type) {
  switch (blockage_type) {
  case LOCAL_BLOCKAGE:
    return local_blockage_logger_;
  case REMOTE_BLOCKAGE:
    return remote_blockage_logger_;
  default:
    return nullptr;
  }
}

void InputLoggerProxy::log_timeslice_blockage(enum LOG_TYPE blockage_type,
                                              /*uint64_t interval_index,*/
                                              uint32_t destination_index,
                                              uint64_t timeslice) {
  IntervalEventDurationSumLogger* logger = get_blockage_object(blockage_type);
  assert(logger != nullptr);
  logger->log_event_start(destination_index, timeslice);
}

void InputLoggerProxy::log_timeslice_completion(enum LOG_TYPE blockage_type,
                                                uint64_t interval_index,
                                                uint32_t destination_index,
                                                uint64_t timeslice) {
  IntervalEventDurationSumLogger* logger = get_blockage_object(blockage_type);
  assert(logger != nullptr);
  logger->log_event_completion(interval_index, destination_index, timeslice);
}

std::vector<uint64_t>
InputLoggerProxy::retrieve_sum_blockage(enum LOG_TYPE blockage_type,
                                        uint64_t interval_index) {
  IntervalEventDurationSumLogger* logger = get_blockage_object(blockage_type);
  assert(logger != nullptr);
  return logger->retrieve_calculated_events_duration(interval_index);
}

uint64_t InputLoggerProxy::get_sum_blockage(enum LOG_TYPE blockage_type,
                                            uint64_t interval_index,
                                            uint32_t destination_index) {
  IntervalEventDurationSumLogger* logger = get_blockage_object(blockage_type);
  assert(logger != nullptr);
  return logger->get_calculated_events_duration(interval_index,
                                                destination_index);
}

void InputLoggerProxy::log_message_transmission(/*uint64_t interval_index,*/
                                                uint32_t destination_index,
                                                uint64_t message_id) {
  timeslice_message_latency_->log_event_start(destination_index, message_id);
}

void InputLoggerProxy::log_message_ACK_arrival(uint64_t interval_index,
                                               uint32_t destination_index,
                                               uint64_t message_id) {
  timeslice_message_latency_->log_event_completion(
      interval_index, destination_index, message_id);
}

std::vector<uint64_t>
InputLoggerProxy::retrieve_message_median_latency(uint64_t interval_index) {
  return timeslice_message_latency_->retrieve_calculated_events_duration(
      interval_index);
}

uint64_t
InputLoggerProxy::get_message_median_latency(uint64_t interval_index,
                                             uint32_t destination_index) {
  return timeslice_message_latency_->get_calculated_events_duration(
      interval_index, destination_index);
}

uint64_t InputLoggerProxy::get_next_message_event_id() {
  return timeslice_message_latency_->get_next_event_id();
}

void InputLoggerProxy::log_rdma_transmission(/*uint64_t interval_index,*/
                                             uint32_t destination_index,
                                             uint64_t message_id) {
  timeslice_write_latency_->log_event_start(destination_index, message_id);
}

void InputLoggerProxy::log_rdma_ACK_arrival(uint64_t interval_index,
                                            uint32_t destination_index,
                                            uint64_t message_id) {
  timeslice_write_latency_->log_event_completion(interval_index,
                                                 destination_index, message_id);
}

std::vector<uint64_t>
InputLoggerProxy::retrieve_rdma_median_latency(uint64_t interval_index) {
  return timeslice_write_latency_->retrieve_calculated_events_duration(
      interval_index);
}

uint64_t InputLoggerProxy::get_rdma_median_latency(uint64_t interval_index,
                                                   uint32_t destination_index) {
  return timeslice_write_latency_->get_calculated_events_duration(
      interval_index, destination_index);
}

void InputLoggerProxy::generate_log_files() {
  timeslice_message_latency_->generate_log_files();
  timeslice_write_latency_->generate_log_files();
  local_blockage_logger_->generate_log_files();
  remote_blockage_logger_->generate_log_files();
}

//// Variables
InputLoggerProxy* InputLoggerProxy::instance_ = nullptr;

} // namespace tl_libfabric
