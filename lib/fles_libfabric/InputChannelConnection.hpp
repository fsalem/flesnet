// Copyright 2012-2013 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "ComputeNodeInfo.hpp"
#include "ComputeNodeStatusMessage.hpp"
#include "Connection.hpp"
#include "InputChannelStatusMessage.hpp"
#include "InputIntervalInfo.hpp"
#include "InputSchedulerOrchestrator.hpp"
#include "TimesliceComponentDescriptor.hpp"

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

    /// Increment target write pointers after data has been sent.
    void check_inc_write_pointers();

    // Get number of bytes to skip in advance (to avoid buffer wrap)
    uint64_t skip_required(uint64_t data_size);

    void finalize(bool abort);

    bool request_abort_flag() { return recv_status_message_.request_abort; }

    void on_complete_write();

    /// Handle Libfabric receive completion notification.
    void on_complete_recv();

    /// Handle Libfabric send completion notification.
    void on_complete_send();

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

    void set_time_MPI(const std::chrono::high_resolution_clock::time_point time_MPI) { send_status_message_.local_time = time_MPI; }

    void reconnect();

    void set_partner_addr(struct fid_av* av_);

    fi_addr_t get_partner_addr();

    void set_remote_info();

    /// Get the last sent timeslice
    const uint64_t& get_last_sent_timeslice() const { return last_sent_timeslice_; }

    /// Set the last sent timeslice
    void set_last_sent_timeslice(uint64_t sent_ts);

    /// Update the status message with the completed interval information
    void ack_complete_interval_info();

    void add_timeslice_data_address(uint64_t data_size, uint64_t desc_size);

    const uint64_t cn_ack_desc() {return cn_ack_.desc;}

    const uint64_t cn_wp_desc() {return cn_wp_.desc;}

private:
    /// Post a receive work request (WR) to the receive queue
    void post_recv_status_message();

    /// Post a send work request (WR) to the send queue
    void post_send_status_message();

    /// This update the last scheduled timeslice, time, and duration
    void update_last_scheduled_info();

    /// Get the median latency of the SYNC messages
    uint64_t get_msg_median_latency();

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

    ///
    ComputeNodeBufferPosition cn_wp_pending_ = ComputeNodeBufferPosition();

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

    /// List of descriptors to be sent in the next sync messages
    std::vector<fles::TimesliceComponentDescriptor> pending_descriptors_;

    /// Data addresses of the pending timeslices
    std::vector<uint64_t> timeslice_data_address_;

    /// count of added descriptors to the sync message
    uint8_t added_sent_descriptors_ = 0;

    /// A ring buffer of SYNC message latency
    std::vector<uint64_t> msg_latency_;

    /// Time that a SYNC message is sent out
    std::chrono::high_resolution_clock::time_point msg_send_time_;

    /// current index to write in the latency ring buffer
    uint16_t msg_latency_index_ = 0;


};
}
