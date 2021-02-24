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
struct InputIntervalMetaDataStatistics {

  // TODO MAX_CONNECTION_COUNT?
  // uint8_t median_buffer_fill_levels[ConstVariables::MAX_CONNECTION_COUNT];
  uint64_t sum_IB_blockage_durations[ConstVariables::MAX_CONNECTION_COUNT];
  uint64_t sum_CB_blockage_durations[ConstVariables::MAX_CONNECTION_COUNT];
  uint64_t median_message_latency[ConstVariables::MAX_CONNECTION_COUNT];
  uint64_t median_rdma_latency[ConstVariables::MAX_CONNECTION_COUNT];

  InputIntervalMetaDataStatistics() {}

  InputIntervalMetaDataStatistics(
      std::vector<uint64_t> sum_compute_blockage_durations,
      std::vector<uint64_t> sum_input_blockage_durations,
      std::vector<uint64_t> median_message_latency,
      std::vector<uint64_t> median_rdma_latency) {
    uint32_t max_size = std::max(sum_compute_blockage_durations.size(),
                                 std::max(sum_input_blockage_durations.size(),
                                          median_rdma_latency.size()));
    for (uint32_t i = 0; i < max_size; i++) {
      if (sum_input_blockage_durations.size() > i)
        sum_IB_blockage_durations[i] = sum_input_blockage_durations[i];
      else
        sum_IB_blockage_durations[i] = 0;

      if (sum_compute_blockage_durations.size() > i)
        sum_CB_blockage_durations[i] = sum_compute_blockage_durations[i];
      else
        sum_CB_blockage_durations[i] = 0;

      if (median_message_latency.size() > i)
        this->median_message_latency[i] = median_message_latency[i];
      else
        this->median_message_latency[i] = 0;

      if (median_rdma_latency.size() > i)
        this->median_rdma_latency[i] = median_rdma_latency[i];
      else
        this->median_rdma_latency[i] = 0;
    }
  }
};
} // namespace tl_libfabric

#pragma pack()
