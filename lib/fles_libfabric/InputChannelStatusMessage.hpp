// Copyright 2014 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "ComputeNodeBufferPosition.hpp"
#include "InputNodeInfo.hpp"
#include "TimesliceComponentDescriptor.hpp"

#include <chrono>

#pragma pack(1)

namespace tl_libfabric {
/// Structure representing a status update message sent from input channel to
/// compute buffer.
struct InputChannelStatusMessage {
  ComputeNodeBufferPosition wp;
  bool abort;
  bool final;
  // "private data" on connect
  bool connect;
  InputNodeInfo info;
  unsigned char my_address[64]; // gni: 50?};

  /// List of descriptors <Descriptor, TimesliceComponentDescriptor>
  std::pair<uint64_t, fles::TimesliceComponentDescriptor>
      tscdesc_msg[ConstVariables::MAX_DESCRIPTOR_ARRAY_SIZE];

  /// Count of the descriptors in the list
  uint16_t descriptor_count = 0;

  bool sync_after_scheduling_decision = false;
  uint32_t failed_index = ConstVariables::MINUS_ONE;
};
} // namespace tl_libfabric

#pragma pack()
