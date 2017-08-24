// Copyright 2012-2013 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "ComputeNodeInfo.hpp"
#include "ComputeNodeStatusMessage.hpp"
#include "Connection.hpp"
#include "InputChannelStatusMessage.hpp"
#include "pid.h"

#include <sys/uio.h>

namespace tl_libfabric
{
/// Input node connection class.
/** An InputChannelConnection object represents the endpoint of a single
 timeslice building connection from an input node to a compute
 node. */

class InputChannelConnection : public Connection
{
public:
    /// The InputChannelConnection constructor.
    InputChannelConnection(struct fid_eq* eq, uint_fast16_t connection_index,
                           uint_fast16_t remote_connection_index,
                           uint_fast16_t remote_connection_count,
                           unsigned int max_send_wr,
                           unsigned int max_pending_write_requests);

    InputChannelConnection(const InputChannelConnection&) = delete;
    void operator=(const InputChannelConnection&) = delete;

    /// Wait until enough space is available at target compute node.
    bool check_for_buffer_space(uint64_t data_size, uint64_t desc_size);

    /// Send data and descriptors to compute node.
    void send_data(struct iovec* sge, void** desc, int num_sge,
                   uint64_t timeslice, uint64_t desc_length,
                   uint64_t data_length, uint64_t skip);

    bool write_request_available();

    /// Increment target write pointers after data has been sent.
    void inc_write_pointers(uint64_t data_size, uint64_t desc_size);

    // Get number of bytes to skip in advance (to avoid buffer wrap)
    uint64_t skip_required(uint64_t data_size);

    void finalize(bool abort);

    bool request_abort_flag() { return recv_status_message_.request_abort; }

    void on_complete_write();

    /// Handle Libfabric receive completion notification.
    void on_complete_recv();

    virtual void setup_mr(struct fid_domain* pd) override;
    virtual void setup() override;

    virtual bool try_sync_buffer_positions() override;

    /// Connection handler function, called on successful connection.
    /**
     \param event RDMA connection manager event structure
     */
    virtual void on_established(struct fi_eq_cm_entry* event) /*override*/;
    //
    void dereg_mr();

    virtual void on_rejected(struct fi_eq_err_entry* event) override;

    virtual void on_disconnected(struct fi_eq_cm_entry* event) /*override*/;

    virtual std::unique_ptr<std::vector<uint8_t>> get_private_data() override;

    void connect(const std::string& hostname, const std::string& service,
                 struct fid_domain* domain, struct fid_cq* cq,
                 struct fid_av* av, fi_addr_t fi_addr);

    void set_time_MPI(const std::chrono::high_resolution_clock::time_point time_MPI) { send_status_message_.MPI_time = time_MPI; }

    void reconnect();

    void set_partner_addr(struct fid_av* av_);

    fi_addr_t get_partner_addr();

    void set_remote_info();

    /// Get the needed duration between sending a timeslice and another
    uint64_t get_wait_time() { return wait_time_; }

    /// set the needed duration between sending a timeslice and another
    void set_wait_time(uint64_t wait_time) { wait_time_ = wait_time; }

    /// Get the last sent timeslice
    const uint64_t& get_last_sent_timeslice() const { return last_sent_timeslice_; }

    /// Set the last sent timeslice
    void set_last_sent_timeslice(uint64_t sent_ts) { last_sent_timeslice_ = sent_ts; }

    /// Add the time of sent a timeslice
    void add_sent_time(uint64_t timeslice, std::chrono::high_resolution_clock::time_point time) { sent_time_list_.add(timeslice, time); }

    /// Check whether a timeslice is sent
    bool contains_sent_time(uint64_t timeslice) const { return sent_time_list_.contains(timeslice); }

    /// get the time when a specific timeslice is sent
    const std::chrono::high_resolution_clock::time_point get_sent_time(uint64_t timeslice) const { return sent_time_list_.get(timeslice); }

    /// Add the scheduled time of sent a timeslice
    void add_scheduled_already_sent_time(uint64_t timeslice, std::chrono::high_resolution_clock::time_point time) { scheduled_time_list_.add(timeslice, time); }

    /// get the scheduled time when a specific timeslice is sent
    const std::chrono::high_resolution_clock::time_point get_scheduled_already_sent_time(uint64_t timeslice) const { return scheduled_time_list_.get(timeslice); }

    /// Add the needed duration to transmit each timeslice and getting the ack back
    void add_sent_duration(uint64_t timeslice, double duration) { sent_duration_list_.add(timeslice, duration); data_acked_ = true;}

    /// Check whether a timeslice is acked
    bool contains_sent_duration(uint64_t timeslice) const { return sent_duration_list_.contains(timeslice); }

    /// Get the needed duration to transmit specific timeslice
    double get_sent_duration(uint64_t timeslice) const { return sent_duration_list_.get(timeslice); }

    /// Return the last acked timeslice
    uint64_t get_last_acked_timeslice();

    /// Return the last scheduled timeslice from the compute node scheduler
    uint64_t get_last_scheduled_timeslice() const { return last_scheduled_timeslice_; }

    /// Return the  time to send the last scheduled timeslice from the compute node scheduler
    std::chrono::high_resolution_clock::time_point get_last_scheduled_time() const { return last_scheduled_time_; }

    /// Calculate when a timeslice should be sent according to the sent duration from the compute node scheduler
    std::chrono::high_resolution_clock::time_point get_scheduled_sent_time(uint64_t timeslice);


private:
    /// Post a receive work request (WR) to the receive queue
    void post_recv_status_message();

    /// Post a send work request (WR) to the send queue
    void post_send_status_message();

    /// Flag, true if it is the input nodes's turn to send a pointer update.
    bool our_turn_ = true;

    bool finalize_ = false;
    bool abort_ = false;

    /// Access information for memory regions on remote end.
    ComputeNodeInfo remote_info_ = ComputeNodeInfo();

    /// Local copy of acknowledged-by-CN pointers
    ComputeNodeBufferPosition cn_ack_ = ComputeNodeBufferPosition();

    /// Receive buffer for CN status (including acknowledged-by-CN pointers)
    ComputeNodeStatusMessage recv_status_message_ = ComputeNodeStatusMessage();

    /// Libfabric memory region descriptor for CN status (including
    /// acknowledged-by-CN pointers)
    fid_mr* mr_recv_ = nullptr;

    /// Local version of CN write pointers
    ComputeNodeBufferPosition cn_wp_ = ComputeNodeBufferPosition();

    /// Send buffer for input channel status (including CN write pointers)
    InputChannelStatusMessage send_status_message_ =
        InputChannelStatusMessage();

    /// Libfabric memory region descriptor for input channel status (including
    /// CN write pointers)
    struct fid_mr* mr_send_ = nullptr;

    /// InfiniBand receive work request
    struct fi_msg recv_wr = fi_msg();
    struct iovec recv_wr_iovec = iovec();
    void* recv_descs[1] = {nullptr};

    /// Infiniband send work request
    struct fi_msg send_wr = fi_msg();
    struct iovec send_wr_iovec = iovec();
    void* send_descs[1] = {nullptr};

    unsigned int pending_write_requests_{0};

    unsigned int max_pending_write_requests_{0};

    uint_fast16_t remote_connection_index_;

    fi_addr_t partner_addr_ = 0;

    uint64_t last_sent_timeslice_ = ConstVariables::MINUS_ONE;

    /// This list of sent timestamp of latest timeslices
    SizedMap<uint64_t, std::chrono::high_resolution_clock::time_point> sent_time_list_;

    /// This list of scheduled timestamp of sent timeslices
    SizedMap<uint64_t, std::chrono::high_resolution_clock::time_point> scheduled_time_list_;

    /// This map contains the spent time to send a receive acknowledgment of timeslices
    SizedMap<uint64_t, double> sent_duration_list_;

    uint64_t last_scheduled_timeslice_ = ConstVariables::MINUS_ONE;
    std::chrono::high_resolution_clock::time_point last_scheduled_time_;
    uint64_t wait_time_ = 500;

};
}
