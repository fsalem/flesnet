// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include <string>
namespace tl_libfabric {
/**
 *
 */
class GenericLogger {
public:
  GenericLogger(bool enable_logging,
                std::string logging_directory,
                std::string logging_file_name);

  ~GenericLogger();

protected:
  bool enable_logging_;
  std::string logging_directory_;
  std::string logging_file_name_;
};
} // namespace tl_libfabric
