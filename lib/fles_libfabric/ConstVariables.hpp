// Copyright 2017 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#pragma pack(1)
namespace tl_libfabric {
struct ConstVariables {
  const static uint32_t MINUS_ONE = -1;
  const static uint32_t ZERO = 0;
  const static uint32_t ONE_HUNDRED = 100;

  const static uint32_t MAX_HISTORY_SIZE =
      50; // high for logging ... should be small ~100-200

  const static uint64_t INIT_HEARTBEAT_TIMEOUT = 1000000; // in microseconds

  const static uint32_t HEARTBEAT_TIMEOUT_HISTORY_SIZE =
      10000; // high for unstable networks

  const static uint32_t HEARTBEAT_TIMEOUT_FACTOR =
      50; // Factor of timeout value before considering a connection timed out

  const static uint32_t HEARTBEAT_INACTIVE_FACTOR =
      30; // Factor of timeout value before considering a connection is in
          // active

  const static uint32_t HEARTBEAT_INACTIVE_RETRY_COUNT = 3;

  const static uint16_t MAX_DESCRIPTOR_ARRAY_SIZE = 10;

  const static uint16_t MAX_COMPUTE_NODE_COUNT = 300;

  const static uint64_t TIMESLICE_TIMEOUT = 200000000; // in microseconds

  const static uint64_t STATUS_MESSAGE_TAG = 10;
  const static uint64_t HEARTBEAT_MESSAGE_TAG = 20;
  const static uint64_t DFS_LB_MESSAGE_TAG = 30;

  // TEMP TODO remove
  const static uint32_t MAX_CONNECTION_COUNT = 15; // TODO max 50??!!

  static std::string array_to_string(uint64_t vec[], uint32_t count) {
    std::stringstream str;
    for (uint32_t i = 0; i < count; i++) {
      str << std::to_string(vec[i]) << " ";
    }
    return str.str();
  }

  static std::string array_to_string(uint32_t vec[], uint32_t count) {
    std::stringstream str;
    for (uint32_t i = 0; i < count; i++) {
      str << std::to_string(vec[i]) << " ";
    }
    return str.str();
  }

  static std::string array_to_string(double vec[], uint32_t count) {
    std::stringstream str;
    for (uint32_t i = 0; i < count; i++) {
      str << std::to_string(vec[i]) << " ";
    }
    return str.str();
  }

  static std::string vector_to_string(std::vector<uint64_t> vec) {
    std::stringstream str;
    for (uint32_t i = 0; i < vec.size(); i++) {
      str << std::to_string(vec[i]) << " ";
    }
    return str.str();
  }

  static std::string vector_to_string(std::vector<double> vec) {
    std::stringstream str;
    for (uint32_t i = 0; i < vec.size(); i++) {
      str << std::to_string(vec[i]) << " ";
    }
    return str.str();
  }

  static std::vector<double> array_to_vector(const double* arr,
                                             uint32_t count) {
    std::vector<double> vec;
    for (uint32_t i = 0; i < count; i++)
      vec.push_back(arr[i]);
    return vec;
  }
};
} // namespace tl_libfabric
#pragma pack()
