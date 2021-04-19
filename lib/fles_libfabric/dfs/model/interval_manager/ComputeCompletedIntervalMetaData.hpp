// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "ComputeIntervalMetaDataStatistics.hpp"
#include "ConstVariables.hpp"
#include "InputIntervalMetaDataStatistics.hpp"
#include "IntervalMetaData.hpp"
#include "log.hpp"

#include <assert.h>
#include <chrono>
#include <vector>

#pragma pack(1)

namespace tl_libfabric {
/// Structure representing the meta-data of intervals that is shared between
/// input and compute nodes.
struct ComputeCompletedIntervalMetaData : public IntervalMetaData {

  ComputeIntervalMetaDataStatistics*
      compute_statistics[ConstVariables::MAX_CONNECTION_COUNT];
  InputIntervalMetaDataStatistics*
      input_statistics[ConstVariables::MAX_CONNECTION_COUNT];

  ComputeCompletedIntervalMetaData() {
    for (uint32_t i = 0; i < ConstVariables::MAX_CONNECTION_COUNT; i++) {
      compute_statistics[i] = nullptr;
      input_statistics[i] = nullptr;
    }
  }

  ComputeCompletedIntervalMetaData(
      uint64_t index,
      uint32_t rounds,
      uint64_t start_ts,
      uint64_t last_ts,
      std::chrono::high_resolution_clock::time_point start_time,
      uint64_t duration,
      uint32_t compute_count)
      : IntervalMetaData(index,
                         rounds,
                         start_ts,
                         last_ts,
                         start_time,
                         duration,
                         compute_count) {
    for (uint32_t i = 0; i < ConstVariables::MAX_CONNECTION_COUNT; i++) {
      compute_statistics[i] = nullptr;
      input_statistics[i] = nullptr;
    }
  }
};
} // namespace tl_libfabric

#pragma pack()
