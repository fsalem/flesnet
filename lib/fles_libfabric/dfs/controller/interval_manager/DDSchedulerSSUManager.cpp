// Copyright 2020 Farouk Salem <salem@zib.de>

#include "DDSchedulerSSUManager.hpp"

namespace tl_libfabric {

// public
DDSchedulerSSUManager::DDSchedulerSSUManager(
    uint32_t scheduler_index,
    double speedup_difference_percentage,
    double speedup_percentage,
    uint32_t speedup_interval_count,
    uint32_t stabalizing_interval_count,
    std::string log_directory,
    bool enable_logging)
    : scheduler_index_(scheduler_index),
      speedup_difference_percentage_(speedup_difference_percentage),
      speedup_percentage_(speedup_percentage),
      init_speedup_percentage_(speedup_percentage),
      speedup_interval_count_(speedup_interval_count),
      stabalizing_interval_count_(stabalizing_interval_count),
      log_directory_(log_directory), enable_logging_(enable_logging) {
  use_variable_speedup_percentage_ = true;
}

//
uint64_t DDSchedulerSSUManager::get_enhanced_duration(
    uint64_t interval_index,
    uint64_t last_completed_interval_index,
    uint64_t init_duration,
    double duration_variance) {
  uint64_t enhanced_duration = init_duration;
  IntervalSchedulerLog* interval_ssu_log = new IntervalSchedulerLog();
  interval_ssu_log->variance = duration_variance;
  interval_ssu_log->variance_percentage =
      (duration_variance / init_duration * 100.0);
  if (within_speedup_phase(interval_index)) { // in speeding up phase
    enhanced_duration = enhanced_interval_duration_;
    interval_ssu_log->phase = IntervalSchedulerLog::Phase::SPEEDUP;
    /*} else if (within_stabalizing_phase(interval_index)) {
      enhanced_duration = stabalizing_enhanced_interval_duration_;
      interval_ssu_log->phase = IntervalSchedulerLog::Phase::STABALIZING;
    } else if (calculate_stabalizing_decision(
                   interval_index, last_completed_interval_index, init_duration,
                   duration_variance, interval_ssu_log->variance_percentage)) {
      enhanced_duration = stabalizing_enhanced_interval_duration_;
      interval_ssu_log->phase = IntervalSchedulerLog::Phase::STABALIZING;*/
  } else if (calculate_speedup_decision(
                 interval_index, last_completed_interval_index, init_duration,
                 duration_variance, interval_ssu_log->variance_percentage)) {
    enhanced_duration = enhanced_interval_duration_;
    interval_ssu_log->phase = IntervalSchedulerLog::Phase::SPEEDUP;
  } else {
    interval_ssu_log->phase = IntervalSchedulerLog::Phase::NONE;
    L_(info) << "[get_enhanced_duration] return the  init_duration: ...";
  }
  /*if (last_completed_interval_index > 7)
    DDLoadBalancerManager::get_instance()->calculate_new_distribtion_load(
        interval_index, last_completed_interval_index - 5,
        last_completed_interval_index - 1);*/
  if (true)
    L_(info) << "[" << scheduler_index_ << "][DDS_SSU] interval "
             << interval_index << " last_completed_interval_index "
             << last_completed_interval_index << " init_duration "
             << init_duration << " enhanced_duration " << enhanced_duration
             << " duration_variance " << duration_variance
             << " variance_percentage " << interval_ssu_log->variance_percentage
             << " phase " << interval_ssu_log->phase_str();
  interval_ssu_log->duration = enhanced_duration;
  interval_scheduler_log_.add(interval_index, interval_ssu_log);
  return enhanced_duration;
}

bool DDSchedulerSSUManager::within_speedup_phase(uint64_t interval_index) {
  if (speedup_interval_index_ != 0 &&
      speedup_interval_index_ + speedup_interval_count_ >
          interval_index) // in speeding up phase
    return true;
  return false;
}

bool DDSchedulerSSUManager::is_speedup_possible(
    uint64_t interval_index,
    uint64_t last_completed_interval_index,
    double duration_variance,
    double variance_percentage) {

  if (speedup_percentage_ > 0 && duration_variance >= 0 &&
      variance_percentage <= speedup_difference_percentage_) {
    // TODO
    /*L_(info) << "[is_speedup_possible] last_completed_interval_index: "
             << last_completed_interval_index
             << " stabalizing+10: " << (stabalizing_interval_count_ + 10);*/
    /*if (last_completed_interval_index > (stabalizing_interval_count_ + 10)) {
      if (!DDLoadBalancerManager::get_instance()->needs_redistribute_load(
              interval_index,
              last_completed_interval_index - stabalizing_interval_count_ - 1,
              last_completed_interval_index - 1))
        return true;
      return false;
    }*/
    return true;
  }

  return false;
}

uint64_t DDSchedulerSSUManager::get_speedup_duration(uint64_t interval_index) {
  if (!within_speedup_phase(interval_index))
    return ConstVariables::MINUS_ONE;
  return enhanced_interval_duration_;
}

bool DDSchedulerSSUManager::within_stabalizing_phase(uint64_t interval_index) {
  if (stabilizing_interval_index_ != 0 &&
      stabilizing_interval_index_ + stabalizing_interval_count_ >
          interval_index)
    return true;
  return false;
}

bool DDSchedulerSSUManager::is_stabalizing_phase_just_finished(
    uint64_t interval_index) {
  if (stabilizing_interval_index_ != 0 &&
      stabilizing_interval_index_ + stabalizing_interval_count_ ==
          interval_index)
    return true;
  return false;
}

uint64_t
DDSchedulerSSUManager::get_stabalizing_duration(uint64_t interval_index) {
  if (!within_stabalizing_phase(interval_index))
    return ConstVariables::MINUS_ONE;
  return stabalizing_enhanced_interval_duration_;
}

// private

bool DDSchedulerSSUManager::calculate_speedup_decision(
    uint64_t interval_index,
    uint64_t last_completed_interval_index,
    uint64_t init_duration,
    double duration_variance,
    double variance_percentage) {

  if (is_speedup_possible(interval_index, last_completed_interval_index,
                          duration_variance, variance_percentage)) {
    if (within_speedup_phase(interval_index - 1)) {
      enhanced_stable_interval_durations_.add(speedup_interval_index_,
                                              enhanced_interval_duration_);
    }
    enhanced_interval_duration_ =
        init_duration * (1.0 - (speedup_percentage_ / 100.0));
    speedup_interval_index_ = interval_index;
    if (true)
      L_(info) << "[" << scheduler_index_ << "][DDS_SSU] interval "
               << interval_index << " starting speeding up for "
               << speedup_interval_count_
               << " intervals[median: " << init_duration
               << ", enhanced: " << enhanced_interval_duration_
               << "] current variance: " << duration_variance
               << " variance percentage: " << variance_percentage
               << " speedup variance percentage: "
               << speedup_difference_percentage_
               << " current speedup percentage: " << speedup_percentage_;

    return true;
  }
  return false;
}

bool DDSchedulerSSUManager::calculate_stabalizing_decision(
    uint64_t interval_index,
    uint64_t last_completed_interval_index,
    uint64_t init_duration,
    double duration_variance,
    double variance_percentage) {

  if (within_speedup_phase(interval_index)/* ||
      !within_speedup_phase(interval_index - 1)*/)
    return false;

  if (is_speedup_possible(interval_index, last_completed_interval_index,
                          duration_variance, variance_percentage)) {
    if (use_variable_speedup_percentage_)
      speedup_percentage_ *= 1.5;
    return false;
  }

  // TODO should be last completed interval not last speedup interval
  std::vector<double> new_load =
      DDLoadBalancerManager::get_instance()->get_last_distribution_load();
  // skip the first stabalizing phase
  if (stabalizing_enhanced_interval_duration_ != 0)
    new_load =
        DDLoadBalancerManager::get_instance()->consider_new_distribtion_load(
            interval_index,
            last_completed_interval_index - stabalizing_interval_count_ - 1,
            last_completed_interval_index - 1);
  if (enhanced_stable_interval_durations_.empty() ||
      !within_speedup_phase(interval_index - 1) ||
      within_stabalizing_phase(interval_index - 1))
    stabalizing_enhanced_interval_duration_ = init_duration;
  else {
    stabalizing_enhanced_interval_duration_ =
        enhanced_stable_interval_durations_.get(
            enhanced_stable_interval_durations_.get_last_key());
  }
  speedup_percentage_ = init_speedup_percentage_;
  use_variable_speedup_percentage_ = false;
  stabilizing_interval_index_ = interval_index;
  if (true) {
    std::stringstream str;
    for (uint32_t i = 0; i < new_load.size(); i++) {
      str << std::to_string(new_load[i]) << " ";
    }

    L_(info) << "[" << scheduler_index_ << "][DDS_SSU] interval "
             << interval_index << " starting Stabilizing phase for "
             << stabalizing_interval_count_
             << " intervals[median: " << init_duration
             << ", proposed: " << stabalizing_enhanced_interval_duration_
             << "] current variance: " << duration_variance
             << " variance percentage: " << variance_percentage
             << " speedup variance percentage: "
             << speedup_difference_percentage_
             << " new speedup percentage: " << speedup_percentage_
             << " new load: " << str.str();
  }

  return true;
}

void DDSchedulerSSUManager::generate_log_files() {
  if (!enable_logging_)
    return;

  std::ofstream log_file;
  log_file.open(log_directory_ + "/" + std::to_string(scheduler_index_) +
                ".compute.ssu_phases.out");

  log_file << std::setw(25) << "Interval" << std::setw(25) << "Duration"
           << std::setw(25) << "Variance" << std::setw(25) << "Var_percentage"
           << std::setw(25) << "Phase"
           << "\n";

  for (SizedMap<uint64_t, IntervalSchedulerLog*>::iterator it =
           interval_scheduler_log_.get_begin_iterator();
       it != interval_scheduler_log_.get_end_iterator(); ++it) {
    log_file << std::setw(25) << it->first << std::setw(25)
             << it->second->duration << std::setw(25) << it->second->variance
             << std::setw(25) << it->second->variance_percentage
             << std::setw(25) << it->second->phase_str() << "\n";
  }

  log_file.flush();
  log_file.close();
}

} // namespace tl_libfabric
