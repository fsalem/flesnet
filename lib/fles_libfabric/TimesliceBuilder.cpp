// Copyright 2013 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#include "TimesliceBuilder.hpp"
#include "ChildProcessManager.hpp"
//#include "InputNodeInfo.hpp"
#include "RequestIdentifier.hpp"
#include "TimesliceCompletion.hpp"
#include "TimesliceWorkItem.hpp"

#include <boost/algorithm/string.hpp>
#include <iomanip>
//#include <boost/lexical_cast.hpp>
//#include <log.hpp>
//#include <random>


//#include <valgrind/memcheck.h>

namespace tl_libfabric
{

TimesliceBuilder::TimesliceBuilder(uint64_t compute_index,
                                   TimesliceBuffer& timeslice_buffer,
                                   unsigned short service,
                                   uint32_t num_input_nodes,
                                   uint32_t timeslice_size,
                                   volatile sig_atomic_t* signal_status,
                                   bool drop, std::string local_node_name,
				   uint32_t scheduler_history_size,
				   uint32_t scheduler_interval_duration,
				   uint32_t scheduler_speedup_difference_percentage,
				   uint32_t scheduler_speedup_percentage,
				   std::string log_directory, bool enable_logging)
    : ConnectionGroup(local_node_name, false), compute_index_(compute_index),
      timeslice_buffer_(timeslice_buffer), service_(service),
      num_input_nodes_(num_input_nodes), timeslice_size_(timeslice_size),
      ack_(timeslice_buffer_.get_desc_size_exp()),
      signal_status_(signal_status), local_node_name_(local_node_name),
      drop_(drop), log_directory_(log_directory)
{
    listening_cq_ = nullptr;
    assert(timeslice_buffer_.get_num_input_nodes() == num_input_nodes);
    assert(not local_node_name_.empty());
    if (Provider::getInst()->is_connection_oriented()) {
        connection_oriented_ = true;
    } else {
        connection_oriented_ = false;
    }
    timeslice_scheduler_ = DDScheduler::get_instance(compute_index, num_input_nodes,
	    scheduler_history_size, scheduler_interval_duration,
	    scheduler_speedup_difference_percentage,
	    scheduler_speedup_percentage, log_directory, enable_logging);
}

TimesliceBuilder::~TimesliceBuilder() {}

void TimesliceBuilder::report_status()
{
    constexpr auto interval = std::chrono::seconds(1);

    std::chrono::system_clock::time_point now =
        std::chrono::system_clock::now();

    L_(debug) << "[c" << compute_index_ << "] " << completely_written_
              << " completely written, " << acked_ << " acked";

    for (auto& c : conn_) {
        auto status_desc = c->buffer_status_desc();
        auto status_data = c->buffer_status_data();
        L_(debug) << "[c" << compute_index_ << "] desc "
                  << status_desc.percentages() << " (used..free) | "
                  << human_readable_count(status_desc.acked, true, "")
                  << " timeslices";
        L_(debug) << "[c" << compute_index_ << "] data "
                  << status_data.percentages() << " (used..free) | "
                  << human_readable_count(status_data.acked, true);
        L_(info) << "[c" << compute_index_ << "_" << c->index() << "] |"
                 << bar_graph(status_data.vector(), "#._", 20) << "|"
                 << bar_graph(status_desc.vector(), "#._", 10) << "| ";
    }

    scheduler_.add(std::bind(&TimesliceBuilder::report_status, this),
                   now + interval);
}

void TimesliceBuilder::request_abort()
{

    L_(info) << "[c" << compute_index_ << "] "
             << "request abort";

    for (auto& connection : conn_) {
        connection->request_abort();
    }
}

void TimesliceBuilder::bootstrap_with_connections()
{
    accept(local_node_name_, service_, num_input_nodes_);
    int rc = MPI_Barrier(MPI_COMM_WORLD);
    assert(rc == MPI_SUCCESS);
    while (connected_ != num_input_nodes_) {
        poll_cm_events();
    }
}

// @todo duplicate code
void TimesliceBuilder::make_endpoint_named(struct fi_info* info,
                                           const std::string& hostname,
                                           const std::string& service,
                                           struct fid_ep** ep)
{

    uint64_t requested_key = 0;
    int res;

    struct fi_info* info2 = nullptr;
    struct fi_info* hints = Provider::get_hints(info->ep_attr->type, info->fabric_attr->prov_name);//fi_dupinfo(info);

    /*hints->caps = info->caps;
     hints->ep_attr = info->ep_attr;
     hints->domain_attr->data_progress = info->domain_attr;
     hints->domain_attr->threading = info->domain_attr->threading;
     hints->domain_attr->mr_mode = info->domain_attr->mr_mode;
     hints->fabric_attr = info->fabric_attr;*/
    /* todo
     hints->rx_attr->size = max_recv_wr_;
     hints->rx_attr->iov_limit = max_recv_sge_;
     hints->tx_attr->size = max_send_wr_;
     hints->tx_attr->iov_limit = max_send_sge_;
     hints->tx_attr->inject_size = max_inline_data_;


    hints->src_addr = nullptr;
    hints->src_addrlen = 0;
    hints->dest_addr = nullptr;
    hints->dest_addrlen = 0;
    */

    int err = fi_getinfo(FI_VERSION(1, 1), hostname.c_str(), service.c_str(),
                         FI_SOURCE, hints, &info2);
    if (err) {
        L_(fatal) << "fi_getinfo failed in make_endpoint: " << err << "="
                  << fi_strerror(-err);
        throw LibfabricException("fi_getinfo failed in make_endpoint");
    }

    fi_freeinfo(hints);

    // private cq for listening ep
    struct fi_cq_attr cq_attr;
    memset(&cq_attr, 0, sizeof(cq_attr));
    cq_attr.size = num_cqe_;
    cq_attr.flags = 0;
    cq_attr.format = FI_CQ_FORMAT_CONTEXT;
    cq_attr.wait_obj = FI_WAIT_NONE;
    cq_attr.signaling_vector = Provider::vector++; // ??
    cq_attr.wait_cond = FI_CQ_COND_NONE;
    cq_attr.wait_set = nullptr;
    res = fi_cq_open(pd_, &cq_attr, &listening_cq_, nullptr);
    if (!listening_cq_) {
        L_(fatal) << "fi_cq_open failed: " << res << "=" << fi_strerror(-res);
        throw LibfabricException("fi_cq_open failed");
    }

    err = fi_endpoint(pd_, info2, ep, this);
    if (err) {
        L_(fatal) << "fi_cq_open failed: " << err << "=" << fi_strerror(-err);
        throw LibfabricException("fi_endpoint failed");
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
    if (Provider::getInst()->has_eq_at_eps()) {
        err = fi_ep_bind(*ep, (::fid_t)eq_, 0);
        if (err) {
            L_(fatal) << "fi_ep_bind failed (eq_): " << err << "="
                      << fi_strerror(-err);
            throw LibfabricException("fi_ep_bind failed (eq_)");
        }
    }
    err = fi_ep_bind(*ep, (::fid_t)listening_cq_, FI_SEND | FI_RECV);
    if (err) {
        L_(fatal) << "fi_ep_bind failed (cq): " << err << "="
                  << fi_strerror(-err);
        throw LibfabricException("fi_ep_bind failed (cq)");
    }
    if (Provider::getInst()->has_av()) {
        err = fi_ep_bind(*ep, (fid_t)av_, 0);
        if (err) {
            L_(fatal) << "fi_ep_bind failed (av): " << err << "="
                      << fi_strerror(-err);
            throw LibfabricException("fi_ep_bind failed (av)");
        }
    }
#pragma GCC diagnostic pop
    err = fi_enable(*ep);
    if (err) {
        L_(fatal) << "fi_enable failed: " << err << "=" << fi_strerror(-err);
        throw LibfabricException("fi_enable failed");
    }

    // register memory regions
    res = fi_mr_reg(pd_, &recv_connect_message_,
                    sizeof(InputChannelStatusMessage), FI_RECV, 0,
                    requested_key++, 0, &mr_recv_, nullptr);

    if (res) {
        L_(fatal) << "fi_mr_reg failed: " << res << "=" << fi_strerror(-res);
        throw LibfabricException("fi_mr_reg failed");
    }

    if (!mr_recv_)
        throw LibfabricException(
            "registration of memory region failed in TimesliceBuilder");
}

void TimesliceBuilder::bootstrap_wo_connections()
{
    InputChannelStatusMessage recv_connect_message;
    struct fid_mr* mr_recv_connect = nullptr;
    struct fi_msg recv_msg_wr;
    struct iovec recv_sge = iovec();
    void* recv_wr_descs[1] = {nullptr};

    // domain, cq, av
    init_context(Provider::getInst()->get_info(), {}, {});

    // listening endpoint with private cq
    make_endpoint_named(Provider::getInst()->get_info(), local_node_name_,
                        std::to_string(service_), &ep_);

    // setup connection objects
    for (size_t index = 0; index < conn_.size(); index++) {
        uint8_t* data_ptr = timeslice_buffer_.get_data_ptr(index);
        fles::TimesliceComponentDescriptor* desc_ptr =
            timeslice_buffer_.get_desc_ptr(index);

        std::unique_ptr<ComputeNodeConnection> conn(new ComputeNodeConnection(
            eq_, pd_, cq_, av_, index, compute_index_, data_ptr,
            timeslice_buffer_.get_data_size_exp(), desc_ptr,
            timeslice_buffer_.get_desc_size_exp()));
        conn->setup_mr(pd_);
        conn->setup();
        conn_.at(index) = std::move(conn);
    }

    // register memory regions
    int err = fi_mr_reg(
        pd_, &recv_connect_message, sizeof(recv_connect_message), FI_RECV, 0,
        Provider::requested_key++, 0, &mr_recv_connect, nullptr);
    if (err) {
        L_(fatal) << "fi_mr_reg failed for recv msg in compute-buffer: " << err
                  << "=" << fi_strerror(-err);
        throw LibfabricException(
            "fi_mr_reg failed for recv msg in compute-buffer");
    }

    // prepare recv message
    recv_sge.iov_base = &recv_connect_message;
    recv_sge.iov_len = sizeof(recv_connect_message);

    recv_wr_descs[0] = fi_mr_desc(mr_recv_);

    recv_msg_wr.msg_iov = &recv_sge;
    recv_msg_wr.desc = recv_wr_descs;
    recv_msg_wr.iov_count = 1;
    recv_msg_wr.addr = FI_ADDR_UNSPEC;
    recv_msg_wr.context = 0;
    recv_msg_wr.data = 0;

    err = fi_recvmsg(ep_, &recv_msg_wr, FI_COMPLETION);
    if (err) {
        L_(fatal) << "fi_recvmsg failed: " << strerror(err);
        throw LibfabricException("fi_recvmsg failed");
    }
    int rc = MPI_Barrier(MPI_COMM_WORLD);
    assert(rc == MPI_SUCCESS);

    // wait for messages from InputChannelSenders
    const int ne_max = 1; // the ne_max must be always 1 because there is ONLY
                          // ONE mr registered variable for receiving messages

    struct fi_cq_entry wc[ne_max];
    int ne;

    while (connected_senders_.size() != num_input_nodes_) {

        while ((ne = fi_cq_read(listening_cq_, &wc, ne_max))) {
            if ((ne < 0) && (ne != -FI_EAGAIN)) {
                L_(fatal) << "fi_cq_read failed: " << ne << "="
                          << fi_strerror(-ne);
                throw LibfabricException("fi_cq_read failed");
            }
            if (ne == FI_SEND) {
                continue;
            }

            if (ne == -FI_EAGAIN)
                break;

            L_(debug) << "got " << ne << " events";
            for (int i = 0; i < ne; ++i) {
                fi_addr_t connection_addr;
                // when connect message:
                //            add address to av and set fi_addr_t from av on
                //            conn-object
                assert(recv_connect_message.connect == true);
                if (connected_senders_.find(recv_connect_message.info.index) !=
                    connected_senders_.end()) {
                    conn_.at(recv_connect_message.info.index)->send_ep_addr();
                    continue;
                }
                int res = fi_av_insert(av_, &recv_connect_message.my_address, 1,
                                       &connection_addr, 0, NULL);
                assert(res == 1);
                // conn_.at(recv_connect_message.info.index)->on_complete_recv();
                conn_.at(recv_connect_message.info.index)
                    ->set_partner_addr(connection_addr);
                conn_.at(recv_connect_message.info.index)
                    ->set_remote_info(recv_connect_message.info);
                conn_.at(recv_connect_message.info.index)->send_ep_addr();
                connected_senders_.insert(recv_connect_message.info.index);
                ++connected_;
                err = fi_recvmsg(ep_, &recv_msg_wr, FI_COMPLETION);
            }
        }
        if (err) {
            L_(fatal) << "fi_recvmsg failed: " << strerror(err);
            throw LibfabricException("fi_recvmsg failed");
        }
    }
}

/// The thread main function.
void TimesliceBuilder::operator()()
{
    try {
        // set_cpu(0);

        if (connection_oriented_) {
            bootstrap_with_connections();
        } else {
            conn_.resize(num_input_nodes_);
            bootstrap_wo_connections();
        }

        int rc = MPI_Barrier(MPI_COMM_WORLD);
	assert(rc == MPI_SUCCESS);
        time_begin_ = std::chrono::system_clock::now();
        timeslice_scheduler_->set_begin_time(time_begin_);

        sync_buffer_positions();
        report_status();
        process_pending_complete_timeslices();
        while (!all_done_ || connected_ != 0) {
            if (!all_done_) {
                poll_completion();
                poll_ts_completion();
            }
            if (connected_ != 0) {
                poll_cm_events();
            }
            scheduler_.timer();
            if (*signal_status_ != 0) {
                *signal_status_ = 0;
                request_abort();
            }
        }

        time_end_ = std::chrono::system_clock::now();

        timeslice_buffer_.send_end_work_item();
        timeslice_buffer_.send_end_completion();

        timeslice_scheduler_->generate_log_files();

        build_time_file();
        summary();
    } catch (std::exception& e) {
        L_(error) << "exception in TimesliceBuilder: " << e.what();
    }
}

void TimesliceBuilder::build_time_file(){

    if (true){
	std::ofstream log_file;
	log_file.open(log_directory_+"/"+std::to_string(compute_index_)+".compute.arrival_diff.out");

	log_file << std::setw(25) << "Timeslice" << std::setw(25) << "Diff" << "\n";
	std::map<uint64_t,double>::iterator it = first_last_arrival_diff_.begin();
	while(it != first_last_arrival_diff_.end()){
	    log_file << std::setw(25) << it->first << std::setw(25) << it->second << "\n";
	    ++it;
	}

	log_file.flush();
	log_file.close();
    }
}

void TimesliceBuilder::on_connect_request(struct fi_eq_cm_entry* event,
                                          size_t private_data_len)
{

    if (!pd_)
        init_context(event->info, {}, {});

    assert(private_data_len >= sizeof(InputNodeInfo));
    InputNodeInfo remote_info;
    /* pacify strict-aliasing rules */
    memcpy(&remote_info, event->data, sizeof(remote_info));

    uint_fast16_t index = remote_info.index;
    assert(index < conn_.size() && conn_.at(index) == nullptr);

    std::unique_ptr<ComputeNodeConnection> conn(
        new ComputeNodeConnection(eq_, index, compute_index_, remote_info,
                                  timeslice_buffer_.get_data_ptr(index),
                                  timeslice_buffer_.get_data_size_exp(),
                                  timeslice_buffer_.get_desc_ptr(index),
                                  timeslice_buffer_.get_desc_size_exp()));
    conn_.at(index) = std::move(conn);

    conn_.at(index)->on_connect_request(event, pd_, cq_);
}

/// Completion notification event dispatcher. Called by the event loop.
void TimesliceBuilder::on_completion(uint64_t wr_id)
{
    size_t in = wr_id >> 8;
    assert(in < conn_.size());
    switch (wr_id & 0xFF) {
    case ID_SEND_STATUS:
        if (false) {
            L_(trace) << "[c" << compute_index_ << "] "
                      << "[" << in << "] "
                      << "COMPLETE SEND status message";
        }
        conn_[in]->on_complete_send();
        break;

    case ID_SEND_FINALIZE:
        if (!conn_[in]->abort_flag()) {
            assert(timeslice_buffer_.get_num_work_items() == 0);
            assert(timeslice_buffer_.get_num_completions() == 0);
        }
        conn_[in]->on_complete_send();
        conn_[in]->on_complete_send_finalize();
        ++connections_done_;
        all_done_ = (connections_done_ == conn_.size());
        if (!connection_oriented_) {
            on_disconnected(nullptr, in);
        }else{
        	// TODO gni check should be removed
        	if (all_done_ && strcmp(Provider::getInst()->get_info()->fabric_attr->prov_name, "gni") == 0){
				disconnect();
			}
        }

        L_(debug) << "[c" << compute_index_ << "] "
                  << "SEND FINALIZE complete for id " << in
                  << " all_done=" << all_done_;
        break;

    case ID_RECEIVE_STATUS: {
	const uint64_t old_recv = conn_[in]->last_recv_ts_;

        conn_[in]->on_complete_recv();

        const uint64_t new_recv = conn_[in]->last_recv_ts_;

	// LOGGING
        if (true){
	    uint64_t remote_ts_num;
	    std::map<uint64_t, uint32_t>::iterator it;
	    for (uint64_t desc=old_recv+1 ; desc <= new_recv ; desc++){
		remote_ts_num = desc*conn_.size() + compute_index_;
		if (!first_arrival_time_.count(remote_ts_num)){
		    first_arrival_time_.insert(std::pair<uint64_t, std::chrono::high_resolution_clock::time_point>(remote_ts_num, std::chrono::high_resolution_clock::now()));
		    arrival_count_.insert(std::pair<uint64_t, uint32_t>(remote_ts_num,1));
		}else{
		    it = arrival_count_.find(remote_ts_num);
		    if ((it->second + 1) != conn_.size()){
			++it->second;
		    }else{
			first_last_arrival_diff_.insert(std::pair<uint64_t, double>(remote_ts_num,
				std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - first_arrival_time_.find(remote_ts_num)->second).count()));
			first_arrival_time_.erase(remote_ts_num);
			arrival_count_.erase(remote_ts_num);
		    }
		}
	    }
        }// END OF LOGGING

        if (connected_ == conn_.size() && in == red_lantern_) {
            auto new_red_lantern = std::min_element(
                std::begin(conn_), std::end(conn_),
                [](const std::unique_ptr<ComputeNodeConnection>& v1,
                   const std::unique_ptr<ComputeNodeConnection>& v2) {
                    return v1->cn_wp().desc < v2->cn_wp().desc;
                });

            uint64_t new_completely_written = (*new_red_lantern)->cn_wp().desc;
            red_lantern_ = std::distance(std::begin(conn_), new_red_lantern);

//            int last_completed_ts_size = completed_ts.size();
            for (uint64_t tpos = completely_written_;
                 tpos < new_completely_written; ++tpos) {
        	pending_complete_ts_.insert(tpos);
            }
            completely_written_ = new_completely_written;
        }
    }
        break;

    default:
        throw LibfabricException("wc for unknown wr_id");
    }
}

void TimesliceBuilder::poll_ts_completion()
{
    fles::TimesliceCompletion c;
    if (!timeslice_buffer_.try_receive_completion(c))
        return;
    if (c.ts_pos == acked_) {
        do
            ++acked_;
        while (ack_.at(acked_) > c.ts_pos);
        for (auto& connection : conn_)
            connection->inc_ack_pointers(acked_);
    } else
        ack_.at(c.ts_pos) = c.ts_pos;
}

bool TimesliceBuilder::check_complete_timeslices(uint64_t ts_pos)
{
    bool all_received = true;
    for (uint32_t indx = 0 ; indx < conn_.size() ; indx++){
	const fles::TimesliceComponentDescriptor& acked_ts =
		timeslice_buffer_.get_desc(indx, ts_pos);
	/*L_(info) << "[process_pending_complete_timeslices] desc = " << ts_pos
		    << ", acked_ts.size = " << acked_ts.size
		    << ", acked_ts.offset = " << acked_ts.offset
		    << ", acked_ts.num_microslices = " << acked_ts.num_microslices
		    << ", acked_ts.ts_num = " << acked_ts.ts_num;*/
	if (acked_ts.num_microslices == ConstVariables::ZERO || acked_ts.size == ConstVariables::ZERO
		|| (acked_ts.offset + acked_ts.size) < conn_[indx]->cn_ack().data){
	    all_received = false;
	    break;
	}
    }
    return all_received;
}

void TimesliceBuilder::process_pending_complete_timeslices()
{
    //L_(info) << "Start a new round of process_pending_complete_timeslices";
    double time = (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - time_begin_).count())/1000.0;
    uint64_t last_processed_ts = ConstVariables::MINUS_ONE;
    for (uint64_t ts_pos : pending_complete_ts_){
	// check whether all contributions are received!
	//L_(info) << "ts_pos = " << ts_pos;
	if (!check_complete_timeslices(ts_pos)) break;
	if (!drop_) {
	    const fles::TimesliceComponentDescriptor& acked_ts = timeslice_buffer_.get_desc(0, ts_pos);
	    uint64_t ts_index = acked_ts.ts_num;
	    timeslice_buffer_.send_work_item(
		{{ts_index, ts_pos, timeslice_size_,
		  static_cast<uint32_t>(conn_.size())},
		 timeslice_buffer_.get_data_size_exp(),
		 timeslice_buffer_.get_desc_size_exp()});
	} else {
	    timeslice_buffer_.send_completion({ts_pos});
	}
	last_processed_ts = ts_pos;
	completed_ts.push_back(time);
    }
    if (last_processed_ts != ConstVariables::MINUS_ONE){
	std::set<uint64_t>::iterator last_processed_it = pending_complete_ts_.find(last_processed_ts);
	pending_complete_ts_.erase(pending_complete_ts_.begin(), ++last_processed_it);
    }

    //L_(info) << "End of process_pending_complete_timeslices";
    scheduler_.add(std::bind(&TimesliceBuilder::process_pending_complete_timeslices, this),
	    std::chrono::system_clock::now() + std::chrono::milliseconds(0));
}

}
