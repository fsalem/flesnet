// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "SizedMap.hpp"
#include "dfs/controller/load_balancer/DDLoadBalancerManager.hpp"
#include "log.hpp"

#include <stdint.h>
#include <vector>

namespace tl_libfabric {
/**
 *
 */
class DDSchedulerSSUManager {
public:
  DDSchedulerSSUManager(uint32_t scheduler_index,
                        double speedup_difference_percentage,
                        double speedup_percentage,
                        uint32_t speedup_interval_count,
                        uint32_t stabalizing_interval_count,
                        std::string log_directory,
                        bool enable_logging);

  //
  uint64_t get_enhanced_duration(uint64_t interval_index,
                                 uint64_t last_completed_interval_index,
                                 uint64_t init_duration,
                                 double duration_variance);

  //
  bool within_speedup_phase(uint64_t interval_index);

  //
  bool is_speedup_possible(uint64_t interval_index,
                           uint64_t last_completed_interval_index,
                           double duration_variance,
                           double variance_percentage);

  //
  uint64_t get_speedup_duration(uint64_t interval_index);

  //
  bool within_stabalizing_phase(uint64_t interval_index);

  //
  bool is_stabalizing_phase_just_finished(uint64_t interval_index);

  //
  uint64_t get_stabalizing_duration(uint64_t interval_index);

  void generate_log_files();

private:
  //
  bool calculate_speedup_decision(uint64_t interval_index,
                                  uint64_t last_completed_interval_index,
                                  uint64_t init_duration,
                                  double duration_variance,
                                  double variance_percentage);

  //
  bool calculate_stabalizing_decision(uint64_t interval_index,
                                      uint64_t last_completed_interval_index,
                                      uint64_t init_duration,
                                      double duration_variance,
                                      double variance_percentage);

  struct IntervalSchedulerLog {
    enum Phase { SPEEDUP, STABALIZING, NONE };
    uint64_t duration;
    double variance;
    double variance_percentage;
    Phase phase;
    std::string phase_str() {
      switch (phase) {
      case SPEEDUP:
        return "SPEEDUP";
      case STABALIZING:
        return "STABALIZING";
      case NONE:
    	  return "NONE";
      }
      return "";
    }
  };
  //
  uint32_t scheduler_index_;

  // max variance percentage between proposed and actual duration to speedup
  double speedup_difference_percentage_;

  // The current speedup percentage
  double speedup_percentage_;

  // The speedup percentage
  double init_speedup_percentage_;

  // The number of intervals to keep speeding up
  uint32_t speedup_interval_count_;

  // The interval number when speeding up is started
  uint64_t speedup_interval_index_ = 0;

  // The enhanced interval duration
  uint64_t enhanced_interval_duration_ = 0;

  // The number of intervals to stabalize after speeding up phase
  uint32_t stabalizing_interval_count_;

  // The last stable enhanced interval duration
  uint64_t stabalizing_enhanced_interval_duration_ = 0;

  // The interval number when stabilizing is started
  uint64_t stabilizing_interval_index_ = 0;

  //
  SizedMap<uint64_t, IntervalSchedulerLog*> interval_scheduler_log_;

  // Enhanced Stable durations <interval, duration>
  SizedMap<uint64_t, uint64_t> enhanced_stable_interval_durations_;

  // Increase speedup_percentage by a factor overtime
  bool use_variable_speedup_percentage_;

  // The log directory
  std::string log_directory_;

  bool enable_logging_;
};
} // namespace tl_libfabric
