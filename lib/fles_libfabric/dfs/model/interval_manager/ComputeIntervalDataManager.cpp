// Copyright 2019 Farouk Salem <salem@zib.de>

#include "ComputeIntervalDataManager.hpp"

namespace tl_libfabric {

ComputeIntervalDataManager*
ComputeIntervalDataManager::get_instance(uint32_t scheduler_index,
                                         uint64_t max_history_size,
                                         std::string log_directory,
                                         bool enable_logging) {
  if (instance_ == nullptr) {
    instance_ = new ComputeIntervalDataManager(
        scheduler_index, max_history_size, log_directory, enable_logging);
  }
  return instance_;
}

ComputeIntervalDataManager* ComputeIntervalDataManager::get_instance() {
  assert(instance_ != nullptr);
  return instance_;
}

ComputeIntervalDataManager::ComputeIntervalDataManager(
    uint32_t scheduler_index,
    uint64_t max_history_size,
    std::string log_directory,
    bool enable_logging)
    : scheduler_index_(scheduler_index), max_history_size_(max_history_size),
      log_directory_(log_directory), enable_logging_(enable_logging) {}

///

//
bool ComputeIntervalDataManager::add_timeslice_completion_duration(
    uint64_t timeslice, double duration) {
  if (contain_timeslice_completion_duration(timeslice))
    return false;
  return timeslice_completion_duration_.add(timeslice, duration);
}

//
double ComputeIntervalDataManager::get_timeslice_completion_duration(
    uint64_t timeslice) {
  assert(contain_timeslice_completion_duration(timeslice));
  return timeslice_completion_duration_.get(timeslice);
}

//
bool ComputeIntervalDataManager::contain_timeslice_completion_duration(
    uint64_t timeslice) {
  return timeslice_completion_duration_.contains(timeslice);
}

//
bool ComputeIntervalDataManager::remove_timeslice_completion_duration(
    uint64_t timeslice) {
  return timeslice_completion_duration_.remove(timeslice);
}

//
bool ComputeIntervalDataManager::add_timeslice_timed_out_duration(
    uint64_t timeslice, double duration) {
  if (contain_timeslice_timed_out_duration(timeslice))
    return false;
  return timeslice_timed_out_.add(timeslice, duration);
}

//
double ComputeIntervalDataManager::get_timeslice_timed_out_duration(
    uint64_t timeslice) {
  assert(contain_timeslice_timed_out_duration(timeslice));
  return timeslice_timed_out_.get(timeslice);
}

//
bool ComputeIntervalDataManager::contain_timeslice_timed_out_duration(
    uint64_t timeslice) {
  return timeslice_timed_out_.contains(timeslice);
}

//
bool ComputeIntervalDataManager::remove_timeslice_timed_out_duration(
    uint64_t timeslice) {
  return timeslice_timed_out_.remove(timeslice);
}

bool ComputeIntervalDataManager::add_proposed_interval_meta_data(
    uint64_t interval, ComputeProposedIntervalMetaData* meta_data) {
  if (contain_proposed_interval_meta_data(interval))
    return false;
  return proposed_interval_meta_data_.add(interval, meta_data);
}

//
ComputeProposedIntervalMetaData*
ComputeIntervalDataManager::get_proposed_interval_meta_data(uint64_t interval) {
  assert(contain_proposed_interval_meta_data(interval));
  return proposed_interval_meta_data_.get(interval);
}

//
bool ComputeIntervalDataManager::contain_proposed_interval_meta_data(
    uint64_t interval) {
  return proposed_interval_meta_data_.contains(interval);
}

//
bool ComputeIntervalDataManager::remove_proposed_interval_meta_data(
    uint64_t interval) {
  return proposed_interval_meta_data_.remove(interval);
}

bool ComputeIntervalDataManager::add_actual_interval_meta_data(
    uint64_t interval, ComputeCompletedIntervalMetaData* meta_data) {
  if (contain_actual_interval_meta_data(interval))
    return false;
  return actual_interval_meta_data_.add(interval, meta_data);
}

//
ComputeCompletedIntervalMetaData*
ComputeIntervalDataManager::get_actual_interval_meta_data(uint64_t interval) {
  assert(contain_actual_interval_meta_data(interval));
  return actual_interval_meta_data_.get(interval);
}

//
uint64_t ComputeIntervalDataManager::get_last_actual_interval_index() {
  assert(!is_actual_interval_meta_data_empty());
  return actual_interval_meta_data_.get_last_key();
}

uint64_t ComputeIntervalDataManager::get_actual_interval_meta_data_size() {
  return actual_interval_meta_data_.size();
}

//
bool ComputeIntervalDataManager::contain_actual_interval_meta_data(
    uint64_t interval) {
  return actual_interval_meta_data_.contains(interval);
}

//
bool ComputeIntervalDataManager::remove_actual_interval_meta_data(
    uint64_t interval) {
  return actual_interval_meta_data_.remove(interval);
}

//
bool ComputeIntervalDataManager::is_actual_interval_meta_data_empty() {
  return actual_interval_meta_data_.empty();
}

//
SizedMap<uint64_t, ComputeCompletedIntervalMetaData*>::iterator
ComputeIntervalDataManager::get_actual_interval_meta_data_begin_interator() {
  return actual_interval_meta_data_.get_begin_iterator();
}

//
SizedMap<uint64_t, ComputeCompletedIntervalMetaData*>::iterator
ComputeIntervalDataManager::get_actual_interval_meta_data_end_interator() {
  return actual_interval_meta_data_.get_end_iterator();
}

///
void ComputeIntervalDataManager::generate_log_files() {
  if (!enable_logging_)
    return;
  std::ofstream log_file;

  /// Duration to complete each timeslice
  log_file.open(log_directory_ + "/" + std::to_string(scheduler_index_) +
                ".compute.arrival_diff.out");

  log_file << std::setw(25) << "Timeslice" << std::setw(25) << "Diff"
           << "\n";
  SizedMap<uint64_t, double>::iterator it =
      timeslice_completion_duration_.get_begin_iterator();
  while (it != timeslice_completion_duration_.get_end_iterator()) {
    log_file << std::setw(25) << it->first << std::setw(25) << it->second
             << "\n";
    ++it;
  }

  log_file.flush();
  log_file.close();

  /// Timed out timeslices
  log_file.open(log_directory_ + "/" + std::to_string(scheduler_index_) +
                ".compute.timeout_ts.out");

  log_file << std::setw(25) << "Timeslice" << std::setw(25) << "Duration"
           << "\n";
  it = timeslice_timed_out_.get_begin_iterator();
  while (it != timeslice_timed_out_.get_end_iterator()) {
    log_file << std::setw(25) << it->first << std::setw(25) << it->second
             << "\n";
    ++it;
  }

  log_file.flush();
  log_file.close();
}

ComputeIntervalDataManager* ComputeIntervalDataManager::instance_ = nullptr;
} // namespace tl_libfabric
