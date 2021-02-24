// Copyright 2019 Farouk Salem <salem@zib.de>

#include "ComputeTimesliceManager.hpp"

namespace tl_libfabric {

ComputeTimesliceManager*
ComputeTimesliceManager::get_instance(uint32_t compute_index,
                                      uint32_t input_connection_count,
                                      uint32_t max_interval_length,
                                      std::string log_directory,
                                      bool enable_logging) {
  if (instance_ == nullptr) {
    instance_ = new ComputeTimesliceManager(
        compute_index, input_connection_count, max_interval_length,
        log_directory, enable_logging);
  }
  return instance_;
}

ComputeTimesliceManager* ComputeTimesliceManager::get_instance() {
  assert(instance_ != nullptr);
  return instance_;
}

ComputeTimesliceManager::ComputeTimesliceManager(
    uint32_t compute_index,
    uint32_t input_connection_count,
    uint32_t max_interval_length,
    std::string log_directory,
    bool enable_logging)
    : compute_index_(compute_index),
      input_connection_count_(input_connection_count),
      log_directory_(log_directory), enable_logging_(enable_logging),
      timeslice_first_arrival_time_(max_interval_length),
      timeslice_arrived_count_(max_interval_length) {
  assert(input_connection_count > 0);
  timeout_ = ConstVariables::TIMESLICE_TIMEOUT;
  compute_interval_data_manager_ = ComputeIntervalDataManager::get_instance();
}

void ComputeTimesliceManager::update_input_connection_count(
    uint32_t input_connection_count) {
  assert(input_connection_count > 0);
  input_connection_count_ = input_connection_count;
}

bool ComputeTimesliceManager::log_contribution_arrival(uint32_t connection_id,
                                                       uint64_t timeslice) {
  assert(connection_id < input_connection_count_);

  if (compute_interval_data_manager_->contain_timeslice_timed_out_duration(
          timeslice) ||
      compute_interval_data_manager_->contain_timeslice_completion_duration(
          timeslice)) {
    return false;
  }
  if (!timeslice_first_arrival_time_.contains(timeslice)) {
    timeslice_first_arrival_time_.add(
        timeslice, std::chrono::high_resolution_clock::now());
  }

  std::set<uint32_t>* arrived_count;
  if (timeslice_arrived_count_.contains(timeslice)) {
    arrived_count = timeslice_arrived_count_.get(timeslice);
    if (arrived_count->find(connection_id) != arrived_count->end())
      return false;
    arrived_count->insert(connection_id);
  } else {
    arrived_count = new std::set<uint32_t>();
    arrived_count->insert(connection_id);
    timeslice_arrived_count_.add(timeslice, arrived_count);
  }

  bool completed = false;
  assert(arrived_count->size() <= input_connection_count_);
  if (arrived_count->size() == input_connection_count_) {
    trigger_timeslice_completion(timeslice);
    completed = true;
  }
  arrived_count = nullptr;
  return completed;
}

bool ComputeTimesliceManager::undo_log_contribution_arrival(
    uint32_t connection_id, uint64_t timeslice) {

  if (compute_interval_data_manager_->contain_timeslice_completion_duration(
          timeslice)) {
    std::set<uint32_t>* arrived_count = new std::set<uint32_t>();
    for (uint32_t conn = 0; conn < input_connection_count_; conn++)
      if (conn != connection_id)
        arrived_count->insert(conn);

    timeslice_arrived_count_.add(timeslice, arrived_count);
    uint64_t dur =
        compute_interval_data_manager_->get_timeslice_completion_duration(
            timeslice);
    timeslice_first_arrival_time_.add(
        timeslice, std::chrono::high_resolution_clock::now() -
                       std::chrono::microseconds(dur));
    compute_interval_data_manager_->remove_timeslice_completion_duration(
        timeslice);

    if (last_ordered_timeslice_ > timeslice - 1)
      last_ordered_timeslice_ = timeslice - 1;
    assert(
        compute_interval_data_manager_->contain_timeslice_completion_duration(
            last_ordered_timeslice_));
    return true;
  }

  if (timeslice_arrived_count_.contains(timeslice)) {
    std::set<uint32_t>* arrived_count = timeslice_arrived_count_.get(timeslice);
    if (arrived_count->find(connection_id) != arrived_count->end()) {
      arrived_count->erase(connection_id);
      return true;
    }
  }
  return false;
}

void ComputeTimesliceManager::log_timeout_timeslice() {
  SizedMap<uint64_t, std::chrono::high_resolution_clock::time_point>::iterator
      it;
  double taken_duration;
  uint64_t timeslice;
  while (!timeslice_first_arrival_time_.empty()) {
    it = timeslice_first_arrival_time_.get_begin_iterator();
    taken_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::high_resolution_clock::now() - it->second)
                         .count();
    if (taken_duration < (timeout_ * 1000.0))
      break;
    timeslice = it->first;

    L_(info) << "----Timeslice " << timeslice << " is timed out after "
             << taken_duration << " ms (timeout=" << (timeout_ * 1000.0)
             << " ms) ---";

    compute_interval_data_manager_->add_timeslice_timed_out_duration(
        timeslice, taken_duration);
    assert(timeslice_first_arrival_time_.remove(timeslice));
    assert(timeslice_arrived_count_.remove(timeslice));
  }
}

uint64_t ComputeTimesliceManager::get_last_ordered_completed_timeslice() {
  return last_ordered_timeslice_;
}

void ComputeTimesliceManager::trigger_timeslice_completion(uint64_t timeslice) {
  double duration = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::high_resolution_clock::now() -
                        timeslice_first_arrival_time_.get(timeslice))
                        .count();
  compute_interval_data_manager_->add_timeslice_completion_duration(timeslice,
                                                                    duration);

  timeslice_first_arrival_time_.remove(timeslice);
  timeslice_arrived_count_.remove(timeslice);

  if (last_ordered_timeslice_ == ConstVariables::MINUS_ONE && timeslice == 0)
    last_ordered_timeslice_ = 0;
  while (last_ordered_timeslice_ != ConstVariables::MINUS_ONE &&
         (compute_interval_data_manager_->contain_timeslice_completion_duration(
              last_ordered_timeslice_ + 1) ||
          compute_interval_data_manager_->contain_timeslice_timed_out_duration(
              last_ordered_timeslice_ + 1)))
    ++last_ordered_timeslice_;
  // L_(info) << "----Timeslice " << timeslice << " is COMPLETED after " <<
  // duration << " ms, last_ordered_timeslice_ = " << last_ordered_timeslice_;
}

ComputeTimesliceManager* ComputeTimesliceManager::instance_ = nullptr;
} // namespace tl_libfabric
