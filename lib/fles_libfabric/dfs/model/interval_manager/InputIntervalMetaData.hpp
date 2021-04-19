// Copyright 2018 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "InputIntervalMetaDataStatistics.hpp"
#include "IntervalMetaData.hpp"

#include <assert.h>
#include <chrono>
#include <vector>

#pragma pack(1)

namespace tl_libfabric {
/// Structure representing the meta-data of intervals that is shared between
/// input and compute nodes input channel.
struct InputIntervalMetaData : public IntervalMetaData {

  InputIntervalMetaDataStatistics statistics;

  InputIntervalMetaData() {}

  InputIntervalMetaData(
      uint64_t index,
      uint32_t rounds,
      uint64_t start_ts,
      uint64_t last_ts,
      std::chrono::high_resolution_clock::time_point start_time,
      uint64_t duration,
      uint32_t compute_count,
      std::vector<uint64_t> sum_compute_blockage_durations,
      std::vector<uint64_t> sum_input_blockage_durations,
      std::vector<uint64_t> median_message_latency,
      std::vector<uint64_t> median_rdma_latency)
      : IntervalMetaData(index,
                         rounds,
                         start_ts,
                         last_ts,
                         start_time,
                         duration,
                         compute_count),
        statistics(sum_compute_blockage_durations,
                   sum_input_blockage_durations,
                   median_message_latency,
                   median_rdma_latency) {}
};
} // namespace tl_libfabric

#pragma pack()
