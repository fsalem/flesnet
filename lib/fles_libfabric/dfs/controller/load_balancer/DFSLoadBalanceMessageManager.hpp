// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include "SizedMap.hpp"
#include "dfs/model/load_balancer/DDSLoadBalancerMessage.hpp"
#include "dfs/model/load_balancer/IELoadBalancerMessage.hpp"

#include <vector>

namespace tl_libfabric {
/**
 *
 */
class DFSLoadBalanceMessageManager {
public:
  static DFSLoadBalanceMessageManager* get_instance();

  // Add new pending DFS-LB message
  void add_pending_message(std::uint32_t connection_id,
                           IELoadBalancerMessage* message);

  // Add new pending DFS-LB message
  void add_pending_message(std::uint32_t connection_id,
                           DDSLoadBalancerMessage* message);

  // Get one of the pending messages, if there
  IELoadBalancerMessage* get_pending_IE_message(std::uint32_t connection_id);

  // Get one of the pending messages, if there
  DDSLoadBalancerMessage* get_pending_DDS_message(std::uint32_t connection_id);

private:
  DFSLoadBalanceMessageManager();
  // Pending messages to be sent
  SizedMap<uint32_t, std::vector<IELoadBalancerMessage*>> pending_IE_messages_;

  SizedMap<uint32_t, std::vector<DDSLoadBalancerMessage*>>
      pending_DDS_messages_;

  static DFSLoadBalanceMessageManager* instance_;
};
} // namespace tl_libfabric
