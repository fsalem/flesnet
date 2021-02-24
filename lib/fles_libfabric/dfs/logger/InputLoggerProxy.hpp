// Copyright 2020 Farouk Salem <salem@zib.de>

#pragma once

#include "IntervalBufferLogger.hpp"
#include "IntervalEventDurationMedianLogger.hpp"
#include "IntervalEventDurationSumLogger.hpp"
#include "log.hpp"

namespace tl_libfabric {
/**
 *  Facade layer of Logger functions
 */
class InputLoggerProxy {
public:
  enum LOG_TYPE { LOCAL_BLOCKAGE, REMOTE_BLOCKAGE };

  static InputLoggerProxy* get_instance();

  static InputLoggerProxy* init_instance(uint32_t destination_count);
  ///////////////// BLOCKAGE  METHODS/////////////////
  /**
   * Log the time when a timeslice is blocked to be transmitted
   */
  void log_timeslice_blockage(enum LOG_TYPE blockage_type,
                              /*uint64_t interval_index,*/
                              uint32_t destination_index,
                              uint64_t timeslice);

  /**
   * Log the time when a timeslice is not blocked and can be transmitted
   */
  void log_timeslice_completion(enum LOG_TYPE blockage_type,
                                uint64_t interval_index,
                                uint32_t destination_index,
                                uint64_t timeslice);

  /**
   * Retrieve the sum blockage for a particular interval and
   * it automatically triggers end of an interval
   *
   */
  std::vector<uint64_t> retrieve_sum_blockage(enum LOG_TYPE blockage_type,
                                              uint64_t interval_index);

  /**
   * Retrieve the sum blockage of a particular interval and destination index.
   * It automatically triggers end of an interval
   *
   */
  uint64_t get_sum_blockage(enum LOG_TYPE blockage_type,
                            uint64_t interval_index,
                            uint32_t destination_index);

  ///////////////// Latency METHODS/////////////////
  /**
   * Log the time when a message is transmitted
   */
  void log_message_transmission(/*uint64_t interval_index,*/
                                uint32_t destination_index,
                                uint64_t message_id);

  /**
   * Log the time when a message ack is received
   */
  void log_message_ACK_arrival(uint64_t interval_index,
                               uint32_t destination_index,
                               uint64_t message_id);

  /**
   * Retrieve the median latency for a particular interval and
   * it automatically triggers end of an interval
   *
   */
  std::vector<uint64_t>
  retrieve_message_median_latency(uint64_t interval_index);

  /**
   * Retrieve the median latency of a particular interval and destination index.
   * It automatically triggers end of an interval
   *
   */
  uint64_t get_message_median_latency(uint64_t interval_index,
                                      uint32_t destination_index);

  /**
   * Get the next unique message id
   */
  uint64_t get_next_message_event_id();

  /**
   * Log the time when a message is transmitted
   */
  void log_rdma_transmission(/*uint64_t interval_index,*/
                             uint32_t destination_index,
                             uint64_t message_id);

  /**
   * Log the time when a message ack is received
   */
  void log_rdma_ACK_arrival(uint64_t interval_index,
                            uint32_t destination_index,
                            uint64_t message_id);

  /**
   * Retrieve the median latency for a particular interval and
   * it automatically triggers end of an interval
   *
   */
  std::vector<uint64_t> retrieve_rdma_median_latency(uint64_t interval_index);

  /**
   * Retrieve the median latency of a particular interval and destination index.
   * It automatically triggers end of an interval
   *
   */
  uint64_t get_rdma_median_latency(uint64_t interval_index,
                                   uint32_t destination_index);

private:
  InputLoggerProxy(uint32_t destination_count);
  IntervalEventDurationSumLogger*
  get_blockage_object(enum LOG_TYPE blockage_type);
  //// Variables
  //
  IntervalEventDurationMedianLogger* timeslice_message_latency_;

  //
  IntervalEventDurationMedianLogger* timeslice_write_latency_;

  //
  IntervalEventDurationSumLogger* local_blockage_logger_;

  //
  IntervalEventDurationSumLogger* remote_blockage_logger_;

  //
  static InputLoggerProxy* instance_;
};
} // namespace tl_libfabric
