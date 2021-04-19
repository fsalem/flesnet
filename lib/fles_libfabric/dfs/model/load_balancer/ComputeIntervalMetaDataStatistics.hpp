// Copyright 2018 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "log.hpp"

#include <assert.h>
#include <chrono>
#include <vector>

#pragma pack(1)

namespace tl_libfabric {
/// Structure representing the meta-data statistics of interval
struct ComputeIntervalMetaDataStatistics {

  uint64_t interval_index;
  uint32_t median_buffer_level[ConstVariables::MAX_CONNECTION_COUNT];
  uint32_t median_timeslice_completion_duration;
  uint32_t median_timeslice_processing_duration;

  ComputeIntervalMetaDataStatistics() {
    interval_index = median_timeslice_completion_duration =
        median_timeslice_processing_duration = ConstVariables::ZERO;
    // median_buffer_level = new uint64_t[DESTINATION_COUNT];
  }

  ComputeIntervalMetaDataStatistics(uint64_t interval_indx,
                                    std::vector<uint64_t> buffer_level,
                                    uint64_t median_completion_duration,
                                    uint64_t median_processing_duration)
      : interval_index(interval_indx),
        median_timeslice_completion_duration(median_completion_duration),
        median_timeslice_processing_duration(median_processing_duration) {
    for (uint32_t i = 0; i < buffer_level.size(); i++) {
      if (buffer_level.size() > i)
        median_buffer_level[i] = buffer_level[i];
      else
        median_buffer_level[i] = 0;
    }
  }
};
} // namespace tl_libfabric

#pragma pack()
