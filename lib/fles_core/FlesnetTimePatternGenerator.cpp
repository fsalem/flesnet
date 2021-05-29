// Copyright 2012-2014 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2021 Farouk Salem <salem@zib.de>

#include "FlesnetTimePatternGenerator.hpp"

// TO BE REMOVED
#include <fstream>
#include <iomanip>

void FlesnetTimePatternGenerator::proceed() {
  if (!time_to_fill()) {
    return;
  }
  last_refilled_time = std::chrono::high_resolution_clock::now();
  last_fill_percentage_index_ =
      (last_fill_percentage_index_ + 1) % fill_percentage_size_;
  const DualIndex target_fill_level = target_buffer_fill_level();

  // break unless significant space is available
  if ((write_index_.data - read_index_.data >= target_fill_level.data) ||
      (write_index_.desc - read_index_.desc >= target_fill_level.desc)) {
    return;
  }
  while (true) {
    // check for current time (rate limiting)
    if (delay_ns_ != UINT64_C(0)) {
      auto delta = std::chrono::high_resolution_clock::now() - begin_;
      auto delta_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(delta).count();
      auto required_ns = static_cast<int64_t>(delay_ns_ * write_index_.desc);
      if (delta_ns < required_ns) {
        break;
      }
    }

    unsigned int content_bytes = typical_content_size_;
    if (randomize_sizes_) {
      content_bytes = random_distribution_(random_generator_);
    }
    content_bytes &= ~0x7u; // round down to multiple of sizeof(uint64_t)

    // check for space in data and descriptor buffers
    if ((write_index_.data - read_index_.data + content_bytes >
         target_fill_level.data) ||
        (write_index_.desc - read_index_.desc + 1 > target_fill_level.desc)) {
      break;
    }

    const uint8_t hdr_id =
        static_cast<uint8_t>(fles::HeaderFormatIdentifier::Standard);
    const uint8_t hdr_ver =
        static_cast<uint8_t>(fles::HeaderFormatVersion::Standard);
    const uint16_t eq_id = 0xE001;
    const uint16_t flags = 0x0000;
    const uint8_t sys_id =
        static_cast<uint8_t>(fles::SubsystemIdentifier::FLES);
    const uint8_t sys_ver = static_cast<uint8_t>(
        generate_pattern_ ? fles::SubsystemFormatFLES::BasicRampPattern
                          : fles::SubsystemFormatFLES::Uninitialized);
    uint64_t idx = write_index_.desc;
    uint32_t crc = 0x00000000;
    uint32_t size = content_bytes;
    uint64_t offset = write_index_.data;

    // write to data buffer
    if (generate_pattern_) {
      for (uint64_t i = 0; i < content_bytes; i += sizeof(uint64_t)) {
        uint64_t data_word = (input_index_ << 48L) | i;
        reinterpret_cast<uint64_t&>(data_buffer_.at(write_index_.data)) =
            data_word;
        write_index_.data += sizeof(uint64_t);
        crc ^= (data_word & 0xffffffff) ^ (data_word >> 32L);
      }
    } else {
      write_index_.data += content_bytes;
    }

    // write to descriptor buffer
    const_cast<fles::MicrosliceDescriptor&>(
        desc_buffer_.at(write_index_.desc++)) =
        fles::MicrosliceDescriptor({hdr_id, hdr_ver, eq_id, flags, sys_id,
                                    sys_ver, idx, crc, size, offset});
  }
  log_current_fill_level();
}

bool FlesnetTimePatternGenerator::time_to_fill() {
  auto delta = std::chrono::high_resolution_clock::now() - last_refilled_time;
  auto delta_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(delta).count();
  if (delta_ms < fill_frequency_)
    return false;
  return true;
}

DualIndex FlesnetTimePatternGenerator::target_buffer_fill_level() {
  DualIndex max_fill_level;
  if (!fixed_level_increase_) {
    double fill_percentage =
        ((fill_percentage_[last_fill_percentage_index_] * 1.0) / 100.0);
    max_fill_level = {uint64_t(desc_buffer_.bytes() * fill_percentage),
                      uint64_t(data_buffer_.bytes() * fill_percentage)};

  } else {
    double fill_percentage = ((fill_percentage_[0] * 1.0) / 100.0);
    uint64_t desc = (write_index_.desc - read_index_.desc) +
                    uint64_t(desc_buffer_.bytes() * fill_percentage);
    desc = desc > desc_buffer_.bytes() ? desc_buffer_.bytes() : desc;
    uint64_t data = (write_index_.data - read_index_.data) +
                    uint64_t(data_buffer_.bytes() * fill_percentage);
    data = data > data_buffer_.bytes() ? data_buffer_.bytes() : data;
    max_fill_level = {desc, data};
  }
  return max_fill_level;
}

void FlesnetTimePatternGenerator::log_current_fill_level() {
  auto delta = last_refilled_time - begin_;
  uint64_t delta_ms =
      std::chrono::duration_cast<std::chrono::seconds>(delta).count();
  double fill_level = ((write_index_.data - read_index_.data) * 100.0) /
                      (data_buffer_.bytes() * 1.0);
  time_fill_level_history_.add(delta_ms, fill_level);
}

FlesnetTimePatternGenerator::~FlesnetTimePatternGenerator() {
  if (!enable_logging_)
    return;
  std::ofstream fill_level_log;
  fill_level_log.open(log_directory_ + "/" + std::to_string(input_index_) +
                      ".input.buffer_fill_level.out");

  fill_level_log << std::setw(25) << "Time" << std::setw(25) << "Level"
                 << "\n";

  SizedMap<uint64_t, double>::iterator iterator =
      time_fill_level_history_.get_begin_iterator();
  while (iterator != time_fill_level_history_.get_end_iterator()) {
    fill_level_log << std::setw(25) << iterator->first << std::setw(25)
                   << iterator->second << "\n";
    ++iterator;
  }

  fill_level_log.flush();
  fill_level_log.close();
}
