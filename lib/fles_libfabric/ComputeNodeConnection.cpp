// Copyright 2012-2013 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#include "ComputeNodeConnection.hpp"
#include "ComputeNodeInfo.hpp"
#include "LibfabricException.hpp"
#include "Provider.hpp"
#include "RequestIdentifier.hpp"
#include <cassert>
#include <log.hpp>
#include <rdma/fi_cm.h>
#include <cmath>

namespace tl_libfabric
{

ComputeNodeConnection::ComputeNodeConnection(
    struct fid_eq* eq, uint_fast16_t connection_index,
    uint_fast16_t remote_connection_index, uint_fast16_t remote_connection_count,
    InputNodeInfo remote_info, uint8_t* data_ptr, uint32_t data_buffer_size_exp,
    fles::TimesliceComponentDescriptor* desc_ptr, uint32_t desc_buffer_size_exp,
    TimesliceScheduler* timeslice_scheduler)
    : Connection(eq, connection_index, remote_connection_index, remote_connection_count),
      remote_info_(std::move(remote_info)), data_ptr_(data_ptr),
      data_buffer_size_exp_(data_buffer_size_exp), desc_ptr_(desc_ptr),
      desc_buffer_size_exp_(desc_buffer_size_exp),
      timeslice_scheduler_(timeslice_scheduler)
{
    // send and receive only single StatusMessage struct
    max_send_wr_ = 2; // one additional wr to avoid race (recv before
    // send completion)
    max_send_sge_ = 1;
    max_recv_wr_ = 1;
    max_recv_sge_ = 1;

    if (Provider::getInst()->is_connection_oriented()) {
        connection_oriented_ = true;
    } else {
        connection_oriented_ = false;
    }
}

ComputeNodeConnection::ComputeNodeConnection(
    struct fid_eq* eq, struct fid_domain* pd, struct fid_cq* cq,
    struct fid_av* av, uint_fast16_t connection_index,
    uint_fast16_t remote_connection_index, uint_fast16_t remote_connection_count,
    /*InputNodeInfo remote_info, */ uint8_t* data_ptr,
    uint32_t data_buffer_size_exp, fles::TimesliceComponentDescriptor* desc_ptr,
    uint32_t desc_buffer_size_exp, TimesliceScheduler* timeslice_scheduler)
    : Connection(eq, connection_index, remote_connection_index, remote_connection_count),
      data_ptr_(data_ptr), data_buffer_size_exp_(data_buffer_size_exp),
      desc_ptr_(desc_ptr), desc_buffer_size_exp_(desc_buffer_size_exp),
      timeslice_scheduler_(timeslice_scheduler)
{

    // send and receive only single StatusMessage struct
    max_send_wr_ = 2; // one additional wr to avoid race (recv before
    // send completion)
    max_send_sge_ = 1;
    max_recv_wr_ = 1;
    max_recv_sge_ = 1;

    if (Provider::getInst()->is_connection_oriented()) {
        connection_oriented_ = true;
    } else {
        connection_oriented_ = false;
    }

    //  setup anonymous endpoint
    make_endpoint(Provider::getInst()->get_info(), "", "", pd, cq, av);
}

void ComputeNodeConnection::post_recv_status_message()
{
    if (false) {
        L_(trace) << "[c" << remote_index_ << "] "
                  << "[" << index_ << "] "
                  << "POST RECEIVE status message";
    }
    post_recv_msg(&recv_wr);
}

void ComputeNodeConnection::post_send_status_message()
{
    if (false) {
        L_(trace) << "[c" << remote_index_ << "] "
                  << "[" << index_ << "] "
                  << "POST SEND status_message"
                  << " (ack.desc=" << send_status_message_.ack.desc << ")"
                  << ", (addr=" << send_wr.addr << ")";
    }
    while (pending_send_requests_ >= 2000 /*qp_cap_.max_send_wr*/) {
        throw LibfabricException(
            "Max number of pending send requests exceeded");
    }
    data_acked_ = false;
    data_changed_ = false;
    ++pending_send_requests_;
    post_send_msg(&send_wr);
}

void ComputeNodeConnection::post_send_final_status_message()
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
    send_wr.context = (void*)(ID_SEND_FINALIZE | (index_ << 8));
#pragma GCC diagnostic pop
    post_send_status_message();
}

void ComputeNodeConnection::setup_mr(struct fid_domain* pd)
{
    assert(data_ptr_ && desc_ptr_ && data_buffer_size_exp_ &&
           desc_buffer_size_exp_);

    // register memory regions
    std::size_t data_bytes = UINT64_C(1) << data_buffer_size_exp_;
    std::size_t desc_bytes = (UINT64_C(1) << desc_buffer_size_exp_) *
                             sizeof(fles::TimesliceComponentDescriptor);
    int res = fi_mr_reg(pd, data_ptr_, data_bytes, FI_WRITE | FI_REMOTE_WRITE,
                        0, Provider::requested_key++, 0, &mr_data_, nullptr);
    if (res) {
        L_(fatal) << "fi_mr_reg failed for data_ptr: " << res << "="
                  << fi_strerror(-res);
        throw LibfabricException("fi_mr_reg failed for data_ptr");
    }
    res = fi_mr_reg(pd, desc_ptr_, desc_bytes, FI_WRITE | FI_REMOTE_WRITE, 0,
                    Provider::requested_key++, 0, &mr_desc_, nullptr);
    if (res) {
        L_(fatal) << "fi_mr_reg failed for desc_ptr: " << res << "="
                  << fi_strerror(-res);
        throw LibfabricException("fi_mr_reg failed for desc_ptr");
    }
    res =
        fi_mr_reg(pd, &send_status_message_, sizeof(ComputeNodeStatusMessage),
                  FI_SEND, 0, Provider::requested_key++, 0, &mr_send_, nullptr);
    if (res) {
        L_(fatal) << "fi_mr_reg failed for send: " << res << "="
                  << fi_strerror(-res);
        throw LibfabricException("fi_mr_reg failed for send");
    }
    res =
        fi_mr_reg(pd, &recv_status_message_, sizeof(InputChannelStatusMessage),
                  FI_RECV, 0, Provider::requested_key++, 0, &mr_recv_, nullptr);
    if (res) {
        L_(fatal) << "fi_mr_reg failed for recv: " << res << "="
                  << fi_strerror(-res);
        throw LibfabricException("fi_mr_reg failed for recv");
    }

    if (!mr_data_ || !mr_desc_ || !mr_recv_ || !mr_send_)
        throw LibfabricException(
            "registration of memory region failed in ComputeNodeConnection");

    send_status_message_.info.data.addr =
        reinterpret_cast<uintptr_t>(data_ptr_);
    send_status_message_.info.data.rkey = fi_mr_key(mr_data_);
    send_status_message_.info.desc.addr =
        reinterpret_cast<uintptr_t>(desc_ptr_);
    send_status_message_.info.desc.rkey = fi_mr_key(mr_desc_);
    send_status_message_.info.index = remote_index_;
    send_status_message_.info.data_buffer_size_exp = data_buffer_size_exp_;
    send_status_message_.info.desc_buffer_size_exp = desc_buffer_size_exp_;
    send_status_message_.connect = false;
}

void ComputeNodeConnection::setup()
{
    // setup send and receive buffers
    recv_sge.iov_base = &recv_status_message_;
    recv_sge.iov_len = sizeof(InputChannelStatusMessage);

    recv_wr_descs[0] = fi_mr_desc(mr_recv_);

    recv_wr.msg_iov = &recv_sge;
    recv_wr.desc = recv_wr_descs;
    recv_wr.iov_count = 1;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
    recv_wr.context = (void*)(ID_RECEIVE_STATUS | (index_ << 8));
#pragma GCC diagnostic pop

    send_sge.iov_base = &send_status_message_;
    send_sge.iov_len = sizeof(ComputeNodeStatusMessage);

    send_wr_descs[0] = fi_mr_desc(mr_send_);

    send_wr.msg_iov = &send_sge;
    send_wr.desc = send_wr_descs;
    send_wr.iov_count = 1;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
    send_wr.context = (void*)(ID_SEND_STATUS | (index_ << 8));
#pragma GCC diagnostic pop

    // post initial receive request
    post_recv_status_message();
}

void ComputeNodeConnection::on_established(struct fi_eq_cm_entry* event)
{
    Connection::on_established(event);

    L_(debug) << "[c" << remote_index_ << "] "
              << "remote index: " << remote_info_.index;
}

void ComputeNodeConnection::on_disconnected(struct fi_eq_cm_entry* event)
{
    if (connection_oriented_) {
        disconnect();
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
    if (mr_recv_) {
        fi_close((struct fid*)mr_recv_);
        mr_recv_ = nullptr;
    }

    if (mr_send_) {
        fi_close((struct fid*)mr_send_);
        mr_send_ = nullptr;
    }

    if (mr_desc_) {
        fi_close((struct fid*)mr_desc_);
        mr_desc_ = nullptr;
    }

    if (mr_data_) {
        fi_close((struct fid*)mr_data_);
        mr_data_ = nullptr;
    }
#pragma GCC diagnostic pop

    Connection::on_disconnected(event);
}

void ComputeNodeConnection::inc_ack_pointers(uint64_t ack_pos)
{
    cn_ack_.desc = ack_pos;

    const fles::TimesliceComponentDescriptor& acked_ts =
        desc_ptr_[(ack_pos - 1) & ((UINT64_C(1) << desc_buffer_size_exp_) - 1)];

    cn_ack_.data = acked_ts.offset + acked_ts.size;

    data_changed_ = true;
}

bool ComputeNodeConnection::try_sync_buffer_positions()
{

    if ((data_acked_ || data_changed_) && !send_status_message_.final) {
        send_status_message_.ack = cn_ack_;

        /// TODO here, it's assumed that # of input nodes = compute nodes
        uint32_t interval_length = ConstVariables::SCHEDULER_INTERVAL_LENGTH * remote_connection_count_;
        uint32_t send_position = std::floor(ConstVariables::SCHEDULER_INTERVAL_LENGTH / 2) * remote_connection_count_;


        if (data_acked_)
        {
            uint64_t last_complete_ts = timeslice_scheduler_->get_last_complete_ts();

            if (send_status_message_.timeslice_to_send == ConstVariables::MINUS_ONE){
        	assert (last_complete_ts == remote_index_);
		send_status_message_.timeslice_to_send = remote_index_;
		send_status_message_.duration = timeslice_scheduler_->get_ts_duration(send_status_message_.timeslice_to_send);
		send_status_message_.time_to_send = timeslice_scheduler_->get_sent_time(index_,send_status_message_.timeslice_to_send);
            }else{
        	if (last_complete_ts >= send_status_message_.timeslice_to_send + send_position){
		    send_status_message_.timeslice_to_send += interval_length;
		    send_status_message_.duration = timeslice_scheduler_->get_ts_duration(last_complete_ts);
		    send_status_message_.time_to_send = timeslice_scheduler_->get_sent_time(index_,send_status_message_.timeslice_to_send);
        	}else{
        	    data_acked_ = false;
        	}
            }
        }
        if (data_acked_){
            send_interval_times_log_.insert(std::pair<uint64_t,std::chrono::system_clock::time_point>(send_status_message_.timeslice_to_send, std::chrono::high_resolution_clock::now()));
        }
        if (data_acked_ || data_changed_){
	    post_send_status_message();
	    return true;
        }
    }
        return false;
}

void ComputeNodeConnection::on_complete_recv()
{
    if (recv_status_message_.final) {
        // send FINAL status message
        send_status_message_.final = true;
        post_send_final_status_message();
        return;
    }
    if (false) {
        L_(trace) << "[c" << remote_index_ << "] "
                  << "[" << index_ << "] "
                  << "COMPLETE RECEIVE status message"
                  << " (wp.desc=" << recv_status_message_.wp.desc << ")";
    }
    // to handle receiving sync messages out of sent order!
    if (cn_wp_.data < recv_status_message_.wp.data && cn_wp_.desc < recv_status_message_.wp.desc)
    	cn_wp_ = recv_status_message_.wp;

    if (!registered_input_MPI_time) {
    	registered_input_MPI_time = true;
    	timeslice_scheduler_->init_input_index_info(index_,recv_status_message_.MPI_time);
    }

    if (recv_status_message_.sent_timeslice != ConstVariables::MINUS_ONE) {
    	timeslice_scheduler_->add_input_ts_info(index_, recv_status_message_.sent_timeslice, recv_status_message_.sent_time, recv_status_message_.proposed_time, recv_status_message_.sent_duration);
    }
    post_recv_status_message();
}

void ComputeNodeConnection::on_complete_send() { pending_send_requests_--; }

void ComputeNodeConnection::on_complete_send_finalize() { done_ = true; }

std::unique_ptr<std::vector<uint8_t>> ComputeNodeConnection::get_private_data()
{
    assert(data_ptr_ && desc_ptr_ && data_buffer_size_exp_ &&
           desc_buffer_size_exp_);
    std::unique_ptr<std::vector<uint8_t>> private_data(
        new std::vector<uint8_t>(sizeof(ComputeNodeInfo)));

    ComputeNodeInfo* cn_info =
        reinterpret_cast<ComputeNodeInfo*>(private_data->data());
    cn_info->data.addr = reinterpret_cast<uintptr_t>(data_ptr_);
    cn_info->data.rkey = fi_mr_key(mr_data_);
    cn_info->desc.addr = reinterpret_cast<uintptr_t>(desc_ptr_);
    cn_info->desc.rkey = fi_mr_key(mr_desc_);
    cn_info->index = remote_index_;
    cn_info->data_buffer_size_exp = data_buffer_size_exp_;
    cn_info->desc_buffer_size_exp = desc_buffer_size_exp_;

    return private_data;
}

void ComputeNodeConnection::set_partner_addr(::fi_addr_t addr)
{
    partner_addr_ = addr;
}

void ComputeNodeConnection::set_remote_info(InputNodeInfo remote_info)
{
    this->remote_info_.index = remote_info.index;
}

void ComputeNodeConnection::send_ep_addr()
{

    size_t addr_len = sizeof(send_status_message_.my_address);
    send_status_message_.connect = true;
    int res =
        fi_getname(&ep_->fid, &send_status_message_.my_address, &addr_len);
    assert(res == 0);
    send_wr.addr = partner_addr_;
    ++pending_send_requests_;
    post_send_msg(&send_wr);
}

bool ComputeNodeConnection::is_connection_finalized()
{
    return send_status_message_.final;
}
}
