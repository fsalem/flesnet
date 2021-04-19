// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "ComputeIntervalMetaDataStatistics.hpp"
#include "InputIntervalMetaData.hpp"
#include "InputNodeInfo.hpp"
#include "TimesliceComponentDescriptor.hpp"

#include <chrono>

#pragma pack(1)

namespace tl_libfabric {
struct IELoadBalancerMessage {
  uint64_t message_id;
  InputNodeInfo info;

  InputIntervalMetaData actual_interval_metadata;

  ComputeIntervalMetaDataStatistics
      previous_interval_statistics[ConstVariables::MAX_CONNECTION_COUNT];

  /////
  /// The required interval info
  uint64_t required_interval_index = ConstVariables::MINUS_ONE;

  /// Local time at which actual interval metadata is sent
  std::chrono::high_resolution_clock::time_point local_time;

  //  bool sync_after_scheduling_decision = false;
  //  uint32_t failed_index = ConstVariables::MINUS_ONE;

  // TODO group the following in one struct
  // collected CN statistics
  uint64_t interval_index;
  // uint8_t median_CN_buffer_fill_levels[300][300]; // max of 300 compute
  // nodes? uint64_t sum_CN_blockage_durations[300][300];   // max of 300
  // compute nodes? uint64_t median_CN_duration_to_complete_ts[300]; uint64_t
  // median_CN_duration_to_process_ts[300];
  // Local statistics
};
} // namespace tl_libfabric

#pragma pack()
