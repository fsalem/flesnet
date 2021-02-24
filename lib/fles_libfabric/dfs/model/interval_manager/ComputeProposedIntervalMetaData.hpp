// Copyright 2018 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "IntervalMetaData.hpp"

#include <assert.h>
#include <chrono>
#include <vector>

#pragma pack(1)

namespace tl_libfabric {
/// Structure representing the meta-data of intervals that is shared between
/// inpute and compute nodes input channel.
struct ComputeProposedIntervalMetaData : public IntervalMetaData {

  ComputeProposedIntervalMetaData() {}
  ComputeProposedIntervalMetaData(
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
                         compute_count) {}
};
} // namespace tl_libfabric

#pragma pack()
