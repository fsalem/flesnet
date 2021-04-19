// Copyright 2020 Farouk Salem <salem@zib.de>

#include "DFSLoadBalanceMessageManager.hpp"

namespace tl_libfabric {

DFSLoadBalanceMessageManager::DFSLoadBalanceMessageManager() {}

DFSLoadBalanceMessageManager* DFSLoadBalanceMessageManager::get_instance() {
  if (instance_ == nullptr) {
    instance_ = new DFSLoadBalanceMessageManager();
  }
  return instance_;
}

// Add new pending DFS-LB message
void DFSLoadBalanceMessageManager::add_pending_message(
    std::uint32_t connection_id, IELoadBalancerMessage* message) {
  if (!pending_IE_messages_.contains(connection_id)) {
    pending_IE_messages_.add(connection_id,
                             std::vector<IELoadBalancerMessage*>());
  }
  pending_IE_messages_.get(connection_id).push_back(message);
}

// Add new pending DFS-LB message
void DFSLoadBalanceMessageManager::add_pending_message(
    std::uint32_t connection_id, DDSLoadBalancerMessage* message) {
  if (!pending_DDS_messages_.contains(connection_id)) {
    pending_DDS_messages_.add(connection_id,
                              std::vector<DDSLoadBalancerMessage*>());
  }
  pending_DDS_messages_.get(connection_id).push_back(message);
}

// Get one of the pending messages, if there
IELoadBalancerMessage* DFSLoadBalanceMessageManager::get_pending_IE_message(
    std::uint32_t connection_id) {
  if (!pending_IE_messages_.contains(connection_id) ||
      pending_IE_messages_.get(connection_id).empty()) {
    return nullptr;
  }
  IELoadBalancerMessage* head_message =
      pending_IE_messages_.get(connection_id)[0];
  pending_IE_messages_.get(connection_id)
      .erase(pending_IE_messages_.get(connection_id).begin());
  return head_message;
}

// Get one of the pending messages, if there
DDSLoadBalancerMessage* DFSLoadBalanceMessageManager::get_pending_DDS_message(
    std::uint32_t connection_id) {
  if (!pending_DDS_messages_.contains(connection_id) ||
      pending_DDS_messages_.get(connection_id).empty()) {
    return nullptr;
  }
  DDSLoadBalancerMessage* head_message =
      pending_DDS_messages_.get(connection_id)[0];
  pending_DDS_messages_.get(connection_id)
      .erase(pending_DDS_messages_.get(connection_id).begin());
  return head_message;
}

DFSLoadBalanceMessageManager* DFSLoadBalanceMessageManager::instance_ = nullptr;
} // namespace tl_libfabric
