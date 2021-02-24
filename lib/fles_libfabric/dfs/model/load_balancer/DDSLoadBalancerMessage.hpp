// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "ComputeIntervalMetaDataStatistics.hpp"
#include "ComputeNodeInfo.hpp"
#include "ComputeProposedIntervalMetaData.hpp"
#include "ConstVariables.hpp"

#include <chrono>
#include <string>

#pragma pack(1)

namespace tl_libfabric {
/// Structure representing a status update message sent from compute buffer to
/// input channel.
struct DDSLoadBalancerMessage {
  uint64_t message_id;
  ComputeNodeInfo info;

  ComputeProposedIntervalMetaData proposed_interval_metadata;

  ComputeIntervalMetaDataStatistics last_completed_statistics;

  // TODO
  double load_distribution[ConstVariables::MAX_CONNECTION_COUNT];
};
} // namespace tl_libfabric

#pragma pack()
