// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include <string>
namespace tl_libfabric {
/**
 *
 */
class GenericLogger {
protected:
  GenericLogger(uint64_t scheduler_index,
                std::string log_key,
                std::string log_directory,
                bool enable_logging)
      : scheduler_index_(scheduler_index), log_key_(log_key),
        log_directory_(log_directory), enable_logging_(enable_logging) {}

  uint64_t scheduler_index_;
  std::string log_key_;
  std::string log_directory_;
  bool enable_logging_;
};
} // namespace tl_libfabric
