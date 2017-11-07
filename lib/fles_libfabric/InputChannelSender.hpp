// Copyright 2012-2013 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "ConnectionGroup.hpp"
#include "DualRingBuffer.hpp"
#include "InputChannelConnection.hpp"
#include "RingBuffer.hpp"
#include "InputIntervalInfo.hpp"
#include <boost/format.hpp>
#include <cassert>

#include <rdma/fi_domain.h>
#include <set>
#include <string>
#include <vector>
#include <mutex>

namespace tl_libfabric
{
/// Input buffer and compute node connection container class.
/** An InputChannelSender object represents an input buffer (filled by a
    FLIB) and a group of timeslice building connections to compute
    nodes. */

class InputChannelSender : public ConnectionGroup<InputChannelConnection>
{
public:
    /// The InputChannelSender default constructor.
    InputChannelSender(uint64_t input_index,
                       InputBufferReadInterface& data_source,
                       const std::vector<std::string> compute_hostnames,
                       const std::vector<std::string> compute_services,
                       uint32_t timeslice_size, uint32_t overlap_size,
                       uint32_t max_timeslice_number,
                       std::string input_node_name);

    InputChannelSender(const InputChannelSender&) = delete;
    void operator=(const InputChannelSender&) = delete;

    /// The InputChannelSender default destructor.
    virtual ~InputChannelSender();

    void report_status();

    void sync_data_source(bool schedule);

    virtual void operator()() override;

    // A scheduling calls to send timeslices to each connection
    void check_send_timeslices();

    /// The central function for distributing timeslice data.
    bool try_send_timeslice(uint64_t timeslice);

    std::unique_ptr<InputChannelConnection>
    create_input_node_connection(uint_fast16_t index);

    /// Initiate connection requests to list of target hostnames.
    void connect();

    virtual void on_connected(struct fid_domain* pd) override;

private:
    /// Return target computation node for given timeslice.
    int target_cn_index(uint64_t timeslice);

    /// Handle RDMA_CM_REJECTED event.
    virtual void on_rejected(struct fi_eq_err_entry* event) override;

    /// Return string describing buffer contents, suitable for debug output.
    std::string get_state_string();

    /// Create gather list for transmission of timeslice
    void post_send_data(uint64_t timeslice, int cn, uint64_t desc_offset,
                        uint64_t desc_length, uint64_t data_offset,
                        uint64_t data_length, uint64_t skip);

    /// Completion notification event dispatcher. Called by the event loop.
    virtual void on_completion(uint64_t wc_id) override;

    /// setup connections between nodes
    void bootstrap_with_connections();

    /// setup connections between nodes
    void bootstrap_wo_connections();

    /// Create a new InputIntervalInfo
    InputIntervalInfo* create_interval_info(uint64_t interval_index);
    
    /// Calculate the interval of a specific timslice
    uint64_t get_timeslice_interval(uint64_t timeslice);
    
    /// Calculate the start timeslice of a particular interval
    uint64_t get_interval_start_ts(uint64_t interval_index);
    
    /// Set the proposed information of a specific interval
    void set_interval_proposed_info(InputIntervalInfo* interval_info);
    
    /// Acknowledge the inputConnections about the completion of an interval
    void ack_complete_interval_info(InputIntervalInfo* interval_info);

    /// create and add a new intervalInfo to intervals_info_
    InputIntervalInfo* add_new_interval(uint64_t interval_index);

    uint64_t input_index_;

    /// Libfabric memory region descriptor for input data buffer.
    struct fid_mr* mr_data_ = nullptr;

    /// Libfabric memory region descriptor for input descriptor buffer.
    struct fid_mr* mr_desc_ = nullptr;

    /// Buffer to store acknowledged status of timeslices.
    RingBuffer<uint64_t, true> ack_;

    /// Number of acknowledged microslices. Written to FLIB.
    uint64_t acked_desc_ = 0;

    /// Number of acknowledged data bytes. Written to FLIB.
    uint64_t acked_data_ = 0;

    /// Data source (e.g., FLIB).
    InputBufferReadInterface& data_source_;

    /// Number of sent microslices, for statistics.
    uint64_t sent_desc_ = 0;

    /// Number of sent data bytes, for statistics.
    uint64_t sent_data_ = 0;

    const std::vector<std::string> compute_hostnames_;
    const std::vector<std::string> compute_services_;

    const uint32_t timeslice_size_;
    const uint32_t overlap_size_;
    const uint64_t max_timeslice_number_;

    const uint64_t min_acked_desc_;
    const uint64_t min_acked_data_;

    uint64_t cached_acked_data_ = 0;
    uint64_t cached_acked_desc_ = 0;

    uint64_t start_index_desc_;
    uint64_t start_index_data_;

    uint64_t write_index_desc_ = 0;

    bool abort_ = false;

    uint64_t sent_timeslices_ = ConstVariables::ZERO;

    SizedMap<uint64_t, InputIntervalInfo*> intervals_info_;

    uint64_t current_interval_ = ConstVariables::ZERO;

    struct IntervalRoundDuration{
	uint64_t interval_index;
	uint32_t round_index;
	uint32_t sent_ts;
	uint32_t remaining_sent_ts;
	uint64_t duration;
	uint64_t duration_to_next_round;
	uint32_t input_buffer_problem_count;
	uint32_t compute_buffer_problem_count;
	uint32_t ack_not_received_problem;
    };

    /// LOGGING
    std::map<uint64_t, std::pair<int64_t, int64_t> > proposed_actual_start_times_log_;
    std::map<uint64_t, std::pair<int64_t, int64_t> > proposed_actual_durations_log_;
    std::map<uint64_t, std::vector<int64_t>> proposed_all_start_times_log_;
    std::map<uint64_t, int64_t > scheduler_blocked_times_log_;
    std::vector<IntervalRoundDuration> interval_rounds_info_log_;
    /*
    std::map<uint64_t, std::pair<uint64_t, uint64_t> > scheduler_blocked_times_log_;
    std::map<uint64_t, std::chrono::system_clock::time_point > temp_scheduler_blocked_times_log_;

    std::map<uint64_t, std::pair<uint64_t, uint64_t> > buffer_blocked_times_log_;
    std::map<uint64_t, std::chrono::system_clock::time_point > temp_buffer_blocked_times_log_;

    std::map<uint64_t, std::pair<uint64_t, uint64_t> > ack_blocked_times_log_;
    std::map<uint64_t, std::chrono::system_clock::time_point > temp_ack_blocked_times_log_;
    */
    /// END LOGGING

    void build_scheduled_time_file();

    struct SendBufferStatus {
        std::chrono::system_clock::time_point time;
        uint64_t size;

        uint64_t cached_acked;
        uint64_t acked;
        uint64_t sent;
        uint64_t written;

        int64_t used() const
        {
            assert(sent <= written);
            return written - sent;
        }
        int64_t sending() const
        {
            assert(acked <= sent);
            return sent - acked;
        }
        int64_t freeing() const
        {
            assert(cached_acked <= acked);
            return acked - cached_acked;
        }
        int64_t unused() const
        {
            assert(written <= cached_acked + size);
            return cached_acked + size - written;
        }

        float percentage(int64_t value) const
        {
            return static_cast<float>(value) / static_cast<float>(size);
        }

        std::string caption() const
        {
            return std::string("used/sending/freeing/free");
        }

        std::string percentage_str(int64_t value) const
        {
            boost::format percent_fmt("%4.1f%%");
            percent_fmt % (percentage(value) * 100);
            std::string s = percent_fmt.str();
            s.resize(4);
            return s;
        }

        std::string percentages() const
        {
            return percentage_str(used()) + " " + percentage_str(sending()) +
                   " " + percentage_str(freeing()) + " " +
                   percentage_str(unused());
        }

        std::vector<int64_t> vector() const
        {
            return std::vector<int64_t>{used(), sending(), freeing(), unused()};
        }
    };

    SendBufferStatus previous_send_buffer_status_desc_ = SendBufferStatus();
    SendBufferStatus previous_send_buffer_status_data_ = SendBufferStatus();
};
}
