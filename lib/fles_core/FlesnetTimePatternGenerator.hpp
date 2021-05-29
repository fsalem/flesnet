// Copyright 2012-2014 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2021 Farouk Salem <salem@zib.de>
#pragma once

#include "DualRingBuffer.hpp"
#include "MicrosliceDescriptor.hpp"
#include "RingBuffer.hpp"
#include "RingBufferView.hpp"
#include "SizedMap.hpp"
#include "log.hpp"
#include <algorithm>
#include <chrono>
#include <random>

/**
 * FlesnetTimePatternGenerator refills the buffer each ${fill_frequency}
 * using two different schema:
 * 1. ${fixed_level_increase} = true: after each ${fill_frequency}, the
 *    buffer is refilled by exactly ${fill_percentage[0]}
 * 2. ${fixed_level_increase} = false: after each ${fill_frequency}, make sure
 *    that buffer fill level is up to ${fill_percentage[i]}. The percentage is
 *    taken from ${fill_percentage[i]} using round-robin schema
 */
/// Simple embedded software pattern generator.
class FlesnetTimePatternGenerator : public InputBufferReadInterface {
public:
  /// The FlesnetPatternGenerator constructor.
  FlesnetTimePatternGenerator(std::size_t data_buffer_size_exp,
                              std::size_t desc_buffer_size_exp,
                              uint64_t input_index,
                              uint32_t typical_content_size,
                              bool generate_pattern,
                              bool randomize_sizes,
                              uint64_t delay_ns,
                              uint32_t fill_frequency,
                              bool fixed_level_increase,
                              uint32_t* fill_percentage,
                              uint32_t fill_percentage_size,
                              std::string log_directory,
                              bool enable_logging)
      : data_buffer_(data_buffer_size_exp), desc_buffer_(desc_buffer_size_exp),
        data_buffer_view_(data_buffer_.ptr(), data_buffer_size_exp),
        desc_buffer_view_(desc_buffer_.ptr(), desc_buffer_size_exp),
        input_index_(input_index), generate_pattern_(generate_pattern),
        typical_content_size_(typical_content_size),
        randomize_sizes_(randomize_sizes),
        random_distribution_(typical_content_size), delay_ns_(delay_ns),
        fill_frequency_(fill_frequency),
        fixed_level_increase_(fixed_level_increase),
        fill_percentage_size_(fill_percentage_size),
        last_fill_percentage_index_(fill_percentage_size - 1),
        log_directory_(log_directory), enable_logging_(enable_logging) {
    assert(fill_percentage_size > 0);
    fill_percentage_ = new uint32_t[fill_percentage_size];
    std::copy(fill_percentage, fill_percentage + fill_percentage_size,
              fill_percentage_);

    begin_ = std::chrono::high_resolution_clock::now();
    last_refilled_time = begin_;
  }

  ~FlesnetTimePatternGenerator() override;

  FlesnetTimePatternGenerator(const FlesnetTimePatternGenerator&) = delete;
  void operator=(const FlesnetTimePatternGenerator&) = delete;

  RingBufferView<uint8_t>& data_buffer() override { return data_buffer_view_; }

  RingBufferView<fles::MicrosliceDescriptor>& desc_buffer() override {
    return desc_buffer_view_;
  }

  void proceed() override;

  DualIndex get_write_index() override { return write_index_; }

  bool get_eof() override { return false; }

  void set_read_index(DualIndex new_read_index) override {
    read_index_ = new_read_index;
  }

  DualIndex get_read_index() override { return read_index_; }

private:
  ///
  bool time_to_fill();

  DualIndex target_buffer_fill_level();

  void log_current_fill_level();

  /// Input data buffer.
  RingBuffer<uint8_t> data_buffer_;

  /// Input descriptor buffer.
  RingBuffer<fles::MicrosliceDescriptor, true> desc_buffer_;

  RingBufferView<uint8_t> data_buffer_view_;
  RingBufferView<fles::MicrosliceDescriptor> desc_buffer_view_;

  /// This node's index in the list of input nodes
  uint64_t input_index_;

  bool generate_pattern_;
  uint32_t typical_content_size_;
  bool randomize_sizes_;

  uint32_t fill_frequency_;
  bool fixed_level_increase_;
  uint32_t* fill_percentage_;
  uint32_t fill_percentage_size_;

  uint32_t last_fill_percentage_index_;
  std::chrono::high_resolution_clock::time_point last_refilled_time;
  SizedMap<uint64_t, double>
      time_fill_level_history_; // <time from begin, fill_level>

  /// A pseudo-random number generator.
  std::default_random_engine random_generator_;

  /// Distribution to use in determining data content sizes.
  std::poisson_distribution<unsigned int> random_distribution_;

  uint64_t delay_ns_;
  std::chrono::high_resolution_clock::time_point begin_;

  /// Number of acknowledged data bytes and microslices. Updated by input
  /// node.
  DualIndex read_index_{0, 0};

  /// FLIB-internal number of written microslices and data bytes.
  DualIndex write_index_{0, 0};

  // Logging
  std::string log_directory_;
  bool enable_logging_;
};
