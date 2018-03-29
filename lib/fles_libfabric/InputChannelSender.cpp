// Copyright 2012-2013 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#include "InputChannelSender.hpp"
#include "MicrosliceDescriptor.hpp"
#include "RequestIdentifier.hpp"
#include "Utility.hpp"
#include <cassert>
#include <chrono>
#include <log.hpp>
#include <rdma/fi_domain.h>
#include <iomanip>

namespace tl_libfabric
{
InputChannelSender::InputChannelSender(
    uint64_t input_index, InputBufferReadInterface& data_source,
    const std::vector<std::string> compute_hostnames,
    const std::vector<std::string> compute_services, uint32_t timeslice_size,
    uint32_t overlap_size, uint32_t max_timeslice_number,
    std::string input_node_name)
    : ConnectionGroup(input_node_name, true), input_index_(input_index),
      data_source_(data_source), compute_hostnames_(compute_hostnames),
      compute_services_(compute_services), timeslice_size_(timeslice_size),
      overlap_size_(overlap_size), max_timeslice_number_(max_timeslice_number),
      min_acked_desc_(data_source.desc_buffer().size() / 4),
      min_acked_data_(data_source.data_buffer().size() / 4),
      intervals_info_(ConstVariables::MAX_HISTORY_SIZE),
      INTERVAL_LENGTH_(ceil(ConstVariables::MAX_TIMESLICE_PER_INTERVAL/compute_hostnames.size()))
{

    start_index_desc_ = sent_desc_ = acked_desc_ = cached_sent_desc_ = cached_acked_desc_ =
        data_source.get_read_index().desc;
    start_index_data_ = sent_data_ = acked_data_ = cached_sent_data_ = cached_acked_data_ =
        data_source.get_read_index().data;

    size_t min_ack_buffer_size =
        data_source_.desc_buffer().size() / timeslice_size_ + 1;
    ack_.alloc_with_size(min_ack_buffer_size);
    sent_.alloc_with_size(min_ack_buffer_size);

    if (Provider::getInst()->is_connection_oriented()) {
        connection_oriented_ = true;
    } else {
        connection_oriented_ = false;
    }
}

InputChannelSender::~InputChannelSender()
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
    if (mr_desc_) {
        fi_close((struct fid*)mr_desc_);
        mr_desc_ = nullptr;
    }

    if (mr_data_) {
        fi_close((struct fid*)(mr_data_));
        mr_data_ = nullptr;
    }
#pragma GCC diagnostic pop
}

void InputChannelSender::report_status()
{
    constexpr auto interval = std::chrono::seconds(1);

        // if data_source.written pointers are lagging behind due to lazy updates,
        // use sent value instead
        uint64_t written_desc = data_source_.get_write_index().desc;
        if (written_desc < sent_desc_) {
            written_desc = sent_desc_;
        }
        uint64_t written_data = data_source_.get_write_index().data;
        if (written_data < sent_data_) {
            written_data = sent_data_;
        }

        std::chrono::system_clock::time_point now =
            std::chrono::system_clock::now();
        SendBufferStatus status_desc{now,
                                     data_source_.desc_buffer().size(),
                                     cached_sent_desc_,
                                     sent_desc_,
                                     sent_desc_,
                                     written_desc};
        SendBufferStatus status_data{now,
                                     data_source_.data_buffer().size(),
                                     cached_sent_data_,
                                     sent_data_,
                                     sent_data_,
                                     written_data};

        double delta_t =
            std::chrono::duration<double, std::chrono::seconds::period>(
                status_desc.time - previous_send_buffer_status_desc_.time)
                .count();
        double rate_desc =
            static_cast<double>(status_desc.acked -
                                previous_send_buffer_status_desc_.acked) /
            delta_t;
        double rate_data =
            static_cast<double>(status_data.acked -
                                previous_send_buffer_status_data_.acked) /
            delta_t;

        L_(debug) << "[i" << input_index_ << "] desc " << status_desc.percentages()
                  << " (used..free) | "
                  << human_readable_count(status_desc.acked, true, "") << " ("
                  << human_readable_count(rate_desc, true, "Hz") << ")";

        L_(debug) << "[i" << input_index_ << "] data " << status_data.percentages()
                  << " (used..free) | "
                  << human_readable_count(status_data.acked, true) << " ("
                  << human_readable_count(rate_data, true, "B/s") << ")";

        L_(info) << "[i" << input_index_ << "]   |"
                 << bar_graph(status_data.vector(), "#x._", 20) << "|"
                 << bar_graph(status_desc.vector(), "#x._", 10) << "| "
                 << human_readable_count(rate_data, true, "B/s") << " ("
                 << human_readable_count(rate_desc, true, "Hz") << ")";

        previous_send_buffer_status_desc_ = status_desc;
        previous_send_buffer_status_data_ = status_data;

        scheduler_.add(std::bind(&InputChannelSender::report_status, this),
    now + interval);
}

void InputChannelSender::sync_data_source(uint64_t timeslice)
{
    uint64_t sent_ts = (cached_sent_desc_ - start_index_desc_) / timeslice_size_;
    if (timeslice != sent_ts) {
	// transmission has been reordered, store completion information
	sent_.at(timeslice) = timeslice;
    } else {
	// completion is for earliest pending timeslice, update indices
	do {
	    ++sent_ts;
	} while (sent_.at(sent_ts) > timeslice);

	uint64_t temp_sent_desc = sent_ts * timeslice_size_ + start_index_desc_;
	uint64_t temp_sent_data =
	data_source_.desc_buffer().at(temp_sent_desc - 1).offset +
	data_source_.desc_buffer().at(temp_sent_desc - 1).size;
	if (temp_sent_data >= cached_sent_data_ || temp_sent_desc >= cached_sent_desc_) {
	    cached_sent_data_ = temp_sent_data;
	    cached_sent_desc_ = temp_sent_desc;
	    data_source_.set_read_index(
		    {cached_sent_desc_, cached_sent_data_});
	}
    }
}

uint64_t InputChannelSender::get_timeslice_interval(uint64_t timeslice){
    return (timeslice / (INTERVAL_LENGTH_ * conn_.size())); // fraction will be thrown away
}

uint64_t InputChannelSender::get_interval_start_ts(uint64_t interval_index){
    return interval_index * INTERVAL_LENGTH_ * conn_.size(); // fraction will be thrown away
}

InputIntervalInfo* InputChannelSender::get_scheduler_proposed_info(const uint64_t interval_index){

    InputIntervalInfo* interval_info = nullptr;

    for (uint32_t i=0 ; i< conn_.size() ; i++) {
	interval_info = conn_[i]->get_proposed_interval_info(interval_index);
	if (interval_info != nullptr)break;
    }
    // LOGGING
    if (ConstVariables::ENABLE_LOGGING && interval_info != nullptr) {
	std::vector<int64_t> times_log;

	for (uint32_t i=0 ; i< conn_.size() ; i++) {
	    InputIntervalInfo* proposed_info = conn_[i]->get_proposed_interval_info(interval_index);

	    times_log.push_back(proposed_info == nullptr ? ConstVariables::MINUS_ONE
		    : std::chrono::duration_cast<std::chrono::milliseconds>(proposed_info->proposed_start_time - time_begin_).count());

	    if (proposed_info == nullptr)continue;

	    //assert(interval_info->proposed_start_time == proposed_info->proposed_start_time);
	    //assert(interval_info->proposed_duration == proposed_info->proposed_duration);

	}
	proposed_all_start_times_log_.insert(std::pair<uint64_t, std::vector<int64_t>>(interval_info->index, times_log));
    }
    // END LOGGING

    return interval_info;
}

InputIntervalInfo* InputChannelSender::create_interval_info(uint64_t interval_index){
    InputIntervalInfo* interval_info = get_scheduler_proposed_info(interval_index);

    // Interval meta-data not received from any of the compute schedulers
    if (interval_info == nullptr){
	if (intervals_info_.size() == 0 || !intervals_info_.contains(interval_index-1)){
	    interval_info = new InputIntervalInfo(interval_index, INTERVAL_LENGTH_, 0, std::chrono::high_resolution_clock::now(), 0);
	}else{ // use prev interval information
	    InputIntervalInfo* prev_interval = intervals_info_.get(interval_index-1);
	    interval_info = new InputIntervalInfo(interval_index, prev_interval->round_count, prev_interval->end_ts+1, std::chrono::high_resolution_clock::now(), prev_interval->proposed_duration);
	}
    }else{
	// check if there is no gap in ts due to un-received durations
	InputIntervalInfo* prev_interval = intervals_info_.get(interval_index-1);
	if (interval_info->start_ts != prev_interval->end_ts+1){
	    // LOGGING
	    // END LOGGING
	    interval_info->start_ts = prev_interval->end_ts+1;
	}
    }
    interval_info->end_ts = interval_info->start_ts + (interval_info->round_count*conn_.size()) - 1;

    return interval_info;

}

void InputChannelSender::ack_complete_interval_info(InputIntervalInfo* interval_info) {
    interval_info->actual_duration = std::chrono::duration_cast<std::chrono::microseconds>(
    		std::chrono::high_resolution_clock::now() - interval_info->actual_start_time).count();
    for (auto& c : conn_) {
	c->ack_complete_interval_info(interval_info);
    }

    if (false){
	L_(trace) << "[i " << input_index_ << "] "
		  << "Interval "
		  << interval_info->index
		  << " completed in " << interval_info->actual_duration
		  << " us while the proposed duration is " << interval_info->proposed_duration << " us";
    }

    // LOGGING
    if (ConstVariables::ENABLE_LOGGING){
	proposed_actual_start_times_log_.insert(std::pair<uint64_t, std::pair<int64_t, int64_t> >(interval_info->index,std::pair<int64_t, int64_t>(
		std::chrono::duration_cast<std::chrono::milliseconds>(interval_info->proposed_start_time - time_begin_).count(),
		std::chrono::duration_cast<std::chrono::milliseconds>(interval_info->actual_start_time - time_begin_).count())));

	proposed_actual_durations_log_.insert(std::pair<uint64_t, std::pair<int64_t, int64_t> >(interval_info->index,std::pair<int64_t, int64_t>(
	    interval_info->proposed_duration/1000.0, interval_info->actual_duration/1000.0)));
    }
    // END LOGGING
}

InputIntervalInfo* InputChannelSender::get_current_interval(uint64_t interval_index){
    if (intervals_info_.contains(interval_index))
	return intervals_info_.get(interval_index);

    InputIntervalInfo* interval_info = create_interval_info(interval_index);
    intervals_info_.add(interval_index,interval_info);
    //offset-based
    cur_index_to_send_ = input_index_ % conn_.size();
    return interval_info;

}

void InputChannelSender::check_send_timeslices()
{
    InputIntervalInfo* interval_info = get_current_interval(current_interval_);
    if (interval_info->rounds_counter == ConstVariables::ZERO){
	interval_info->actual_start_time = std::chrono::high_resolution_clock::now();
    }
    interval_info->rounds_counter++;

    if (false){
	L_(trace) << "[i " << input_index_ << "] "
		  << "start a new round of interval "
		  << current_interval_
		  << "(interval_sent_count = " << interval_info->count_sent_ts
		  << " & sent_timeslices = " << sent_timeslices_ << ")";
    }

    std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now(),
	    next_check_time;

    int32_t sent_count = 0,
	    input_buffer_problem_count = 0,
	    compute_buffer_problem_count = 0;

    uint32_t conn_index = input_index_ % conn_.size();
    while (!interval_info->is_interval_sent_completed()){
	uint64_t next_ts = conn_[conn_index]->get_last_sent_timeslice() == ConstVariables::MINUS_ONE ? conn_index :
			    conn_[conn_index]->get_last_sent_timeslice() + conn_.size();

	if (next_ts <= max_timeslice_number_ && interval_info->is_ts_within_current_round(next_ts)){
	    // LOGGING
	    timeslice_delaying_log_.insert(std::pair<uint64_t, int64_t>(sent_timeslices_+1
		    , std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - interval_info->get_expected_sent_time(sent_timeslices_+1)).count()));
	    // END OF LOGGING
	    if (try_send_timeslice(next_ts)){
		if (interval_info->cb_blocked){
		    interval_info->cb_blocked = false;
		    interval_info->cb_blocked_duration += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-interval_info->cb_blocked_start_time).count();
		}
		if (interval_info->ib_blocked){
		    interval_info->ib_blocked = false;
		    interval_info->ib_blocked_duration += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-interval_info->ib_blocked_start_time).count();
		}
		conn_[conn_index]->set_last_sent_timeslice(next_ts);
		conn_[conn_index]->add_sent_time(next_ts, now);
		sent_timeslices_++;
		interval_info->count_sent_ts++;
		sent_count++;
	    }
	    /// LOGGING
	    else{
		uint64_t desc_offset = next_ts * timeslice_size_ + start_index_desc_;
		uint64_t desc_length = timeslice_size_ + overlap_size_;

		if (write_index_desc_ < desc_offset + desc_length) {
		    write_index_desc_ = data_source_.get_write_index().desc;
		}
		// check if microslice no. (desc_offset + desc_length - 1) is avail
		if (write_index_desc_ < desc_offset + desc_length) {
		    if (!interval_info->ib_blocked){
			interval_info->ib_blocked = true;
			interval_info->ib_blocked_start_time = std::chrono::high_resolution_clock::now();
		    }
		    input_buffer_problem_count++;
		}else{
		    if (!interval_info->cb_blocked){
			interval_info->cb_blocked = true;
			interval_info->cb_blocked_start_time = std::chrono::high_resolution_clock::now();
		    }
		    compute_buffer_problem_count++;
		}
	    }
	    /// END LOGGING
	}
	conn_index = (conn_index+1) % conn_.size();
	if (conn_index == (input_index_ % conn_.size()))break;
	std::chrono::high_resolution_clock::time_point time_to_next_ts = interval_info->get_expected_sent_time(
		interval_info->count_sent_ts + 1);
	if (time_to_next_ts > std::chrono::high_resolution_clock::now())
	std::this_thread::sleep_for(std::chrono::microseconds(std::chrono::duration_cast<std::chrono::microseconds>(time_to_next_ts - std::chrono::high_resolution_clock::now()).count()));
    }

    if (interval_info->is_interval_sent_completed() && !interval_info->is_interval_sent_ack_completed() && !is_ack_blocked_){
	is_ack_blocked_ = true;
	ack_blocked_start_time_ = std::chrono::high_resolution_clock::now();
    }
    
    //get_timeslice_interval(sent_timeslices_+1) - 1 ==  interval_info->index
    if (interval_info->is_interval_sent_ack_completed()){
	ack_complete_interval_info(interval_info);
	++current_interval_;
	next_check_time = get_current_interval(current_interval_)->proposed_start_time;
	/// LOGGING
	if (ConstVariables::ENABLE_LOGGING){
	    if (is_ack_blocked_){
		is_ack_blocked_ = false;
		uint64_t ack_blocked_time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - ack_blocked_start_time_).count();
		ack_blocked_times_log_.insert(std::pair<uint64_t, uint64_t>(interval_info->index, ack_blocked_time));
		overall_ACK_blocked_time_ += ack_blocked_time;
	    }
	    int64_t scheduler_blocked_time = std::chrono::duration_cast<std::chrono::microseconds>(next_check_time - std::chrono::high_resolution_clock::now()).count();
	    scheduler_blocked_times_log_.insert(std::pair<uint64_t, int64_t>(current_interval_, scheduler_blocked_time));
	    if (scheduler_blocked_time > 0)
		overall_scheduler_blocked_time_ += scheduler_blocked_time;
	    scheduler_IB_blocked_times_log_.insert(std::pair<uint64_t, uint64_t>(interval_info->index, interval_info->ib_blocked_duration));
	    overall_IB_blocked_time_ += interval_info->ib_blocked_duration;
	    scheduler_CB_blocked_times_log_.insert(std::pair<uint64_t, uint64_t>(interval_info->index, interval_info->cb_blocked_duration));
	    overall_CB_blocked_time_ += interval_info->cb_blocked_duration;
	}
	/// END LOGGING
    }else{
	next_check_time = std::chrono::high_resolution_clock::now() + std::chrono::microseconds(interval_info->get_duration_to_next_round());
    }

    /// LOGGING
    if (ConstVariables::ENABLE_LOGGING){
	interval_rounds_info_log_.push_back(IntervalRoundDuration{interval_info->index, interval_info->rounds_counter,
	sent_count, (interval_info->end_ts-interval_info->start_ts+1-interval_info->count_sent_ts),
	std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-now).count(),
	std::chrono::duration_cast<std::chrono::microseconds>(next_check_time - std::chrono::high_resolution_clock::now()).count(),
	input_buffer_problem_count, compute_buffer_problem_count});

	if (false){
	    L_(info) << "interval = " << interval_info->index << ", round = " << interval_info->rounds_counter <<
		    ", IB = " << input_buffer_problem_count << ", CB = " << compute_buffer_problem_count <<
		    ", sent = " << sent_count << " in " << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - now).count() <<
		    " us , next interval = " << current_interval_ << " after " << std::chrono::duration_cast<std::chrono::microseconds>(next_check_time - now).count() << " us";
	}
    }
    /// END LOGGING
    if (sent_timeslices_ <= max_timeslice_number_){
	if (false){
	    L_(trace) << "[i " << input_index_ << "] "
		      << "check a new round after "
		      << std::chrono::duration_cast<std::chrono::milliseconds>(
				next_check_time - now).count() << " ms";
	}
	scheduler_.add(std::bind(&InputChannelSender::check_send_timeslices, this), next_check_time);
    }
}


void InputChannelSender::check_send_timeslices_TMP()
{
    InputIntervalInfo* interval_info = get_current_interval(current_interval_);
    if (interval_info->rounds_counter == ConstVariables::ZERO){
	interval_info->actual_start_time = std::chrono::high_resolution_clock::now();
    }
    interval_info->rounds_counter++;

    if (false){
	L_(trace) << "[i " << input_index_ << "] "
		  << "start a new round of interval "
		  << current_interval_
		  << "(interval_sent_count = " << interval_info->count_sent_ts
		  << " & sent_timeslices = " << sent_timeslices_ << ")";
    }

    std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now(),
	    next_check_time;

    int32_t sent_count = 0,
	    input_buffer_problem_count = 0,
	    compute_buffer_problem_count = 0;

    if (!interval_info->is_interval_sent_completed()){
	uint64_t next_ts = conn_[cur_index_to_send_]->get_last_sent_timeslice() == ConstVariables::MINUS_ONE ? cur_index_to_send_ :
			    conn_[cur_index_to_send_]->get_last_sent_timeslice() + conn_.size();

	if (next_ts <= max_timeslice_number_ && interval_info->is_ts_within_current_round(next_ts)){
	    // LOGGING
	    timeslice_delaying_log_.insert(std::pair<uint64_t, int64_t>(sent_timeslices_+1
	    		    , std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - interval_info->get_expected_sent_time(sent_timeslices_+1)).count()));
	    // END OF LOGGING
	    if (try_send_timeslice(next_ts)){
		if (interval_info->cb_blocked){
		    interval_info->cb_blocked = false;
		    interval_info->cb_blocked_duration += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-interval_info->cb_blocked_start_time).count();
		}
		if (interval_info->ib_blocked){
		    interval_info->ib_blocked = false;
		    interval_info->ib_blocked_duration += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-interval_info->ib_blocked_start_time).count();
		}
		conn_[cur_index_to_send_]->set_last_sent_timeslice(next_ts);
		conn_[cur_index_to_send_]->add_sent_time(next_ts, now);
		sent_timeslices_++;
		interval_info->count_sent_ts++;
		sent_count++;
	    }
	    /// LOGGING
	    else{
		uint64_t desc_offset = next_ts * timeslice_size_ + start_index_desc_;
		uint64_t desc_length = timeslice_size_ + overlap_size_;

		if (write_index_desc_ < desc_offset + desc_length) {
		    write_index_desc_ = data_source_.get_write_index().desc;
		}
		// check if microslice no. (desc_offset + desc_length - 1) is avail
		if (write_index_desc_ < desc_offset + desc_length) {
		    if (!interval_info->ib_blocked){
			interval_info->ib_blocked = true;
			interval_info->ib_blocked_start_time = std::chrono::high_resolution_clock::now();
		    }
		    input_buffer_problem_count++;
		}else{
		    if (!interval_info->cb_blocked){
			interval_info->cb_blocked = true;
			interval_info->cb_blocked_start_time = std::chrono::high_resolution_clock::now();
		    }
		    compute_buffer_problem_count++;
		}
	    }
	    /// END LOGGING
	}
	cur_index_to_send_ = (cur_index_to_send_+1) % conn_.size();
    }

    if (interval_info->is_interval_sent_completed() && !interval_info->is_interval_sent_ack_completed() && !is_ack_blocked_){
	is_ack_blocked_ = true;
	ack_blocked_start_time_ = std::chrono::high_resolution_clock::now();
    }

    //get_timeslice_interval(sent_timeslices_+1) - 1 ==  interval_info->index
    if (interval_info->is_interval_sent_ack_completed()){
	ack_complete_interval_info(interval_info);
	++current_interval_;
	next_check_time = get_current_interval(current_interval_)->proposed_start_time;
	/// LOGGING
	if (ConstVariables::ENABLE_LOGGING){
	    if (is_ack_blocked_){
		is_ack_blocked_ = false;
		uint64_t ack_blocked_time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - ack_blocked_start_time_).count();
		ack_blocked_times_log_.insert(std::pair<uint64_t, uint64_t>(interval_info->index, ack_blocked_time));
		overall_ACK_blocked_time_ += ack_blocked_time;
	    }
	    int64_t scheduler_blocked_time = std::chrono::duration_cast<std::chrono::microseconds>(next_check_time - std::chrono::high_resolution_clock::now()).count();
	    scheduler_blocked_times_log_.insert(std::pair<uint64_t, int64_t>(current_interval_, scheduler_blocked_time));
	    if (scheduler_blocked_time > 0)
		overall_scheduler_blocked_time_ += scheduler_blocked_time;
	    scheduler_IB_blocked_times_log_.insert(std::pair<uint64_t, uint64_t>(interval_info->index, interval_info->ib_blocked_duration));
	    overall_IB_blocked_time_ += interval_info->ib_blocked_duration;
	    scheduler_CB_blocked_times_log_.insert(std::pair<uint64_t, uint64_t>(interval_info->index, interval_info->cb_blocked_duration));
	    overall_CB_blocked_time_ += interval_info->cb_blocked_duration;
	}
	/// END LOGGING
    }else{
	next_check_time = std::chrono::high_resolution_clock::now() + std::chrono::microseconds(interval_info->get_duration_to_next_ts());
    }

    /// LOGGING
    if (ConstVariables::ENABLE_LOGGING){
	interval_rounds_info_log_.push_back(IntervalRoundDuration{interval_info->index, interval_info->rounds_counter,
	sent_count, (interval_info->end_ts-interval_info->start_ts+1-interval_info->count_sent_ts),
	std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now()-now).count(),
	std::chrono::duration_cast<std::chrono::microseconds>(next_check_time - std::chrono::high_resolution_clock::now()).count(),
	input_buffer_problem_count, compute_buffer_problem_count});

	if (false){
	    L_(info) << "interval = " << interval_info->index << ", round = " << interval_info->rounds_counter <<
		    ", IB = " << input_buffer_problem_count << ", CB = " << compute_buffer_problem_count <<
		    ", sent = " << sent_count << " in " << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - now).count() <<
		    " us , next interval = " << current_interval_ << " after " << std::chrono::duration_cast<std::chrono::microseconds>(next_check_time - now).count() << " us";
	}
    }
    /// END LOGGING
    if (sent_timeslices_ <= max_timeslice_number_){
	if (false){
	    L_(trace) << "[i " << input_index_ << "] "
		      << "check a new round after "
		      << std::chrono::duration_cast<std::chrono::milliseconds>(
				next_check_time - now).count() << " ms";
	}
	scheduler_.add(std::bind(&InputChannelSender::check_send_timeslices_TMP, this), next_check_time);
    }
}

void InputChannelSender::bootstrap_with_connections()
{
    connect();
    while (connected_ != compute_hostnames_.size()) {
        poll_cm_events();
    }
}

void InputChannelSender::bootstrap_wo_connections()
{
    // domain, cq, av
    init_context(Provider::getInst()->get_info(), compute_hostnames_,
                 compute_services_);

    int rc = MPI_Barrier(MPI_COMM_WORLD);
    assert(rc == MPI_SUCCESS);
    conn_.resize(compute_hostnames_.size());
    // setup connections objects
    for (unsigned int i = 0; i < compute_hostnames_.size(); ++i) {
        std::unique_ptr<InputChannelConnection> connection =
            create_input_node_connection(i);
        // creates endpoint
        connection->connect(compute_hostnames_[i], compute_services_[i], pd_,
                            cq_, av_, fi_addrs[i]);
        conn_.at(i) = (std::move(connection));
    }
    int i = 0;
    while (connected_ != compute_hostnames_.size()) {
        poll_completion();
        i++;
        if (i == 1000000) {
            i = 0;
            // reconnecting
            for (unsigned int i = 0; i < compute_hostnames_.size(); ++i) {
                if (connected_indexes_.find(i) == connected_indexes_.end()) {
                    L_(info) << "retrying to connect to "
                             << compute_hostnames_[i] << ":"
                             << compute_services_[i];
                    conn_.at(i)->reconnect();
                }
            }
        }
    }
}

/// The thread main function.
void InputChannelSender::operator()()
{
    try {

        if (Provider::getInst()->is_connection_oriented()) {
            bootstrap_with_connections();
        } else {
            bootstrap_wo_connections();
        }

        data_source_.proceed();
        int rc = MPI_Barrier(MPI_COMM_WORLD);
        assert(rc == MPI_SUCCESS);
        time_begin_ = std::chrono::high_resolution_clock::now();

        for (uint32_t indx = 0 ; indx< conn_.size() ; indx++){
	    conn_[indx]->set_time_MPI(time_begin_);
        }

        sync_buffer_positions();
        report_status();
        check_send_timeslices_TMP();

        while (sent_timeslices_ <= max_timeslice_number_ && !abort_) {
            /*if (try_send_timeslice(sent_timeslices_)) {
            	sent_timeslices_++;
            }*/
            scheduler_.timer();
            poll_completion();
            data_source_.proceed();
        }

        L_(info) << "[i" << input_index_ << "]"
        	<< "All timeslices are sent.  wait for pending send completions!";

        // wait for pending send completions
        while (acked_desc_ < timeslice_size_ * sent_timeslices_ + start_index_desc_) {
            poll_completion();
            scheduler_.timer();
        }

        L_(debug) << "[i " << input_index_ << "] "
                  << "Finalize Connections";
        for (auto& c : conn_) {
            c->finalize(abort_);
        }

        L_(debug) << "[i" << input_index_ << "] "
                  << "SENDER loop done";
        while (!all_done_) {
            poll_completion();
            scheduler_.timer();
        }
        time_end_ = std::chrono::high_resolution_clock::now();
        overall_running_time_ = std::chrono::duration_cast<std::chrono::microseconds>(
                time_end_ - time_begin_)
                .count();

        if (connection_oriented_) {
            disconnect();
        }

        while (connected_ != 0) {
            poll_cm_events();
        }

        summary();
        build_scheduled_time_file();
    } catch (std::exception& e) {
        L_(fatal) << "exception in InputChannelSender: " << e.what();
    }
}

void InputChannelSender::build_scheduled_time_file(){
    if (!ConstVariables::ENABLE_LOGGING) return;
    if (true){
	std::ofstream log_file;
	log_file.open(std::to_string(input_index_)+".input.proposed_actual_interval_info.out");

	log_file << std::setw(25) << "Interval" <<
		std::setw(25) << "proposed time" <<
		std::setw(25) << "Actual time" <<
		std::setw(25) << "Proposed duration" <<
		std::setw(25) << "Actual duration" << "\n";

	std::map<uint64_t, std::pair<int64_t, int64_t> >::iterator it_time = proposed_actual_start_times_log_.begin(),
	    it_dur = proposed_actual_durations_log_.begin();
	while (it_time != proposed_actual_start_times_log_.end() && it_dur != proposed_actual_durations_log_.end()){
	    log_file << std::setw(25) << it_time->first
		    << std::setw(25) << it_time->second.first
		    << std::setw(25) << it_time->second.second
		    << std::setw(25) << it_dur->second.first
		    << std::setw(25) << it_dur->second.second << "\n";

	    it_time++;
	    it_dur++;
	}
	log_file.flush();
	log_file.close();
    }

//////////////////////////////////////////////////////////////////////
    if (true){
	std::ofstream times_log_file;
	times_log_file.open(std::to_string(input_index_)+".input.proposed_all_start_times.out");

	times_log_file << std::setw(25) << "Interval";
	for (int i=0 ; i < conn_.size() ; i++){
	    times_log_file << std::setw(25) << "Compute#" << i;
	}
	times_log_file << "\n";

	std::map<uint64_t, std::vector<int64_t>>::iterator it_interval_time = proposed_all_start_times_log_.begin();
	while (it_interval_time != proposed_all_start_times_log_.end()){
	    times_log_file << std::setw(25) << it_interval_time->first;
	    for (int64_t val:it_interval_time->second){
		times_log_file << std::setw(25) << val;
	    }
	    times_log_file << "\n";

	    it_interval_time++;
	}
	times_log_file.flush();
	times_log_file.close();
    }

/////////////////////////////////////////////////////////////////
    if (true) {
	std::ofstream block_log_file;
	block_log_file.open(std::to_string(input_index_)+".input.scheduler_blocked_times.out");

	block_log_file << std::setw(25) << "Interval" <<
	    std::setw(25) << "blocked duration to start" <<
	    std::setw(25) << "IB blocked duration" <<
	    std::setw(25) << "CB blocked duration" <<
	    std::setw(25) << "ACK blocked duration" << "\n";

	std::map<uint64_t, uint64_t >::iterator it_IB_blocked_time = scheduler_IB_blocked_times_log_.begin(),
		it_CB_blocked_time, ack_blocked_time;
	std::map<uint64_t, int64_t >::iterator it_blocked_time;
	while (it_IB_blocked_time != scheduler_IB_blocked_times_log_.end()){
	    it_CB_blocked_time = scheduler_CB_blocked_times_log_.find(it_IB_blocked_time->first);
	    it_blocked_time = scheduler_blocked_times_log_.find(it_IB_blocked_time->first);
	    ack_blocked_time = ack_blocked_times_log_.find(it_IB_blocked_time->first);
	    block_log_file << std::setw(25) << it_IB_blocked_time->first <<
			    std::setw(25) << (it_blocked_time != scheduler_blocked_times_log_.end() ? it_blocked_time->second/1000.0 : 0) <<
			    std::setw(25) << it_IB_blocked_time->second/1000.0 <<
			    std::setw(25) << it_CB_blocked_time->second/1000.0 <<
			    std::setw(25) << ack_blocked_time->second/1000.0 << "\n";

	    it_IB_blocked_time++;
	}
	block_log_file.flush();
	block_log_file.close();
    }

/////////////////////////////////////////////////////////////////
    if (true) {
	std::ofstream overall_block_log_file;
	overall_block_log_file.open(std::to_string(input_index_)+".input.overall_blocked_times.out");

	overall_block_log_file << "0" << std::setw(40) <<
		"\"overall running time\"" << std::setw(40) <<
		overall_running_time_ << "\n" <<
		"1" << std::setw(40) <<
		"\"scheduler blocked duration\"" << std::setw(40) <<
		overall_scheduler_blocked_time_ << "\n" <<
		"2" << std::setw(40) <<
		"\"IB blocked duration\"" << std::setw(40) <<
		overall_IB_blocked_time_ << "\n" <<
		"3" << std::setw(40) <<
		"\"CB blocked duration\"" << std::setw(40) <<
		overall_CB_blocked_time_ << "\n" <<
		"\"ACK blocked duration\"" << std::setw(40) <<
		overall_ACK_blocked_time_<< "\n";

	overall_block_log_file.flush();
	overall_block_log_file.close();
    }

/////////////////////////////////////////////////////////////////
    if (true) {
    	std::ofstream block_log_file;
    	block_log_file.open(std::to_string(input_index_)+".input.ts_delaying_times.out");

    	block_log_file << std::setw(25) << "Timeslice" <<
    	    std::setw(25) << "duration" << "\n";

    	std::map<uint64_t, int64_t >::iterator delaying_time = timeslice_delaying_log_.begin();
    	while (delaying_time != timeslice_delaying_log_.end()){
    	    block_log_file << std::setw(25) << delaying_time->first <<
    			    std::setw(25) << delaying_time->second/1000.0 << "\n";

    	delaying_time++;
    	}
    	block_log_file.flush();
    	block_log_file.close();
    }

/////////////////////////////////////////////////////////////////
    if (false) {
	std::ofstream duration_log_file;
	duration_log_file.open(std::to_string(input_index_)+".input.ts_duration.out");

	duration_log_file << std::setw(25) << "Timeslice" <<
		std::setw(25) << "Compute Index" <<
		std::setw(25) << "Duration" << "\n";

	for (std::pair<uint64_t, uint64_t> dur: timeslice_duration_log_){
	    duration_log_file << std::setw(25) << dur.first <<
		    std::setw(25) << target_cn_index(dur.first) <<
		    std::setw(25) << dur.second << "\n";
	}
	duration_log_file.flush();
	duration_log_file.close();
    }

/////////////////////////////////////////////////////////////////
    if (false) {
	std::ofstream round_log_file;
	round_log_file.open(std::to_string(input_index_)+".input.interval_round_info.out");

	round_log_file << std::setw(25) << "Interval" <<
		std::setw(25) << "Round" <<
		std::setw(25) << "sent count" <<
		std::setw(25) << "remaining" <<
		std::setw(25) << "duration" <<
		std::setw(25) << "time to next round" <<
		std::setw(25) << "IB problem" <<
		std::setw(25) << "CB problem" << "\n";

	for (IntervalRoundDuration ird : interval_rounds_info_log_){
	    round_log_file << std::setw(25) << ird.interval_index <<
		    std::setw(25) << ird.round_index <<
		    std::setw(25) << ird.sent_ts <<
		    std::setw(25) << ird.remaining_sent_ts <<
		    std::setw(25) << ird.duration <<
		    std::setw(25) << ird.duration_to_next_round <<
		    std::setw(25) << ird.input_buffer_problem_count <<
		    std::setw(25) << ird.compute_buffer_problem_count << "\n";
	}
	round_log_file.flush();
	round_log_file.close();
    }
}

bool InputChannelSender::try_send_timeslice(uint64_t timeslice)
{
    // wait until a complete timeslice is available in the input buffer
    uint64_t desc_offset = timeslice * timeslice_size_ + start_index_desc_;
    uint64_t desc_length = timeslice_size_ + overlap_size_;

    if (write_index_desc_ < desc_offset + desc_length) {
        write_index_desc_ = data_source_.get_write_index().desc;
    }
    // check if microslice no. (desc_offset + desc_length - 1) is avail
    if (write_index_desc_ >= desc_offset + desc_length) {

        uint64_t data_offset =
            data_source_.desc_buffer().at(desc_offset).offset;
        uint64_t data_end =
            data_source_.desc_buffer()
                .at(desc_offset + desc_length - 1)
                .offset +
            data_source_.desc_buffer().at(desc_offset + desc_length - 1).size;
        assert(data_end >= data_offset);

        uint64_t data_length = data_end - data_offset;
        uint64_t total_length =
            data_length + desc_length * sizeof(fles::MicrosliceDescriptor);

        if (false) {
            L_(trace) << "SENDER working on timeslice " << timeslice
                      << ", microslices " << desc_offset << ".."
                      << (desc_offset + desc_length - 1) << ", data bytes "
                      << data_offset << ".." << (data_offset + data_length - 1);
            L_(trace) << get_state_string();
        }

        int cn = target_cn_index(timeslice);

        if (!conn_[cn]->write_request_available()){
            L_(info) << "[" << input_index_ << "]"
        	    << "max # of writes to " << cn;
            return false;
        }

        // number of bytes to skip in advance (to avoid buffer wrap)
        uint64_t skip = conn_[cn]->skip_required(total_length);
        total_length += skip;

        if (conn_[cn]->check_for_buffer_space(total_length, 1)) {

            post_send_data(timeslice, cn, desc_offset, desc_length, data_offset,
                           data_length, skip);

            conn_[cn]->inc_write_pointers(total_length, 1);

            if (data_end > sent_data_){
            	sent_desc_ = desc_offset + desc_length;
            	sent_data_ = data_end;
            }

            sync_data_source(timeslice);
            return true;
        }
    }

    return false;
}

std::unique_ptr<InputChannelConnection>
InputChannelSender::create_input_node_connection(uint_fast16_t index)
{
    // @todo
    // unsigned int max_send_wr = 8000; ???  IB hca
    // unsigned int max_send_wr = 495; // ??? libfabric for verbs
    unsigned int max_send_wr = 256; // ??? libfabric for sockets

    // limit pending write requests so that send queue and completion queue
    // do not overflow
    unsigned int max_pending_write_requests = std::min(
        static_cast<unsigned int>((max_send_wr - 1) / 3),
        static_cast<unsigned int>((num_cqe_ - 1) / compute_hostnames_.size()));

    std::unique_ptr<InputChannelConnection> connection(
        new InputChannelConnection(eq_, index, input_index_, conn_.size(), max_send_wr,
                                   max_pending_write_requests));
    return connection;
}

void InputChannelSender::connect()
{
    if (!pd_) // pd, cq2, av
        init_context(Provider::getInst()->get_info(), compute_hostnames_,
                     compute_services_);

    int rc = MPI_Barrier(MPI_COMM_WORLD);
    assert(rc == MPI_SUCCESS);
    conn_.resize(compute_hostnames_.size());
    for (unsigned int i = 0; i < compute_hostnames_.size(); ++i) {
        std::unique_ptr<InputChannelConnection> connection =
            create_input_node_connection(i);
        connection->connect(compute_hostnames_[i], compute_services_[i], pd_,
                            cq_, av_, FI_ADDR_UNSPEC);
        conn_.at(i) = (std::move(connection));
    }
}

int InputChannelSender::target_cn_index(uint64_t timeslice)
{
    return timeslice % conn_.size();
}

void InputChannelSender::on_connected(struct fid_domain* pd)
{
    if (!mr_data_) {
        // Register memory regions.
        int err = fi_mr_reg(
            pd, const_cast<uint8_t*>(data_source_.data_send_buffer().ptr()),
            data_source_.data_send_buffer().bytes(), FI_WRITE, 0,
            Provider::requested_key++, 0, &mr_data_, nullptr);
        if (err) {
            L_(fatal) << "fi_mr_reg failed for data_send_buffer: " << err << "="
                      << fi_strerror(-err);
            throw LibfabricException("fi_mr_reg failed for data_send_buffer");
        }

        if (!mr_data_) {
            L_(fatal) << "fi_mr_reg failed for mr_data: " << strerror(errno);
            throw LibfabricException("registration of memory region failed");
        }

        err = fi_mr_reg(pd, const_cast<fles::MicrosliceDescriptor*>(
                                data_source_.desc_send_buffer().ptr()),
                        data_source_.desc_send_buffer().bytes(), FI_WRITE, 0,
                        Provider::requested_key++, 0, &mr_desc_, nullptr);
        if (err) {
            L_(fatal) << "fi_mr_reg failed for desc_send_buffer: " << err << "="
                      << fi_strerror(-err);
            throw LibfabricException("fi_mr_reg failed for desc_send_buffer");
        }

        if (!mr_desc_) {
            L_(fatal) << "fi_mr_reg failed for mr_desc: " << strerror(errno);
            throw LibfabricException("registration of memory region failed");
        }
    }
}

void InputChannelSender::on_rejected(struct fi_eq_err_entry* event)
{
    L_(debug) << "InputChannelSender:on_rejected";

    InputChannelConnection* conn =
        static_cast<InputChannelConnection*>(event->fid->context);

    conn->on_rejected(event);
    uint_fast16_t i = conn->index();
    conn_.at(i) = nullptr;

    L_(debug) << "retrying: " << i;
    // immediately initiate retry
    std::unique_ptr<InputChannelConnection> connection =
        create_input_node_connection(i);
    connection->connect(compute_hostnames_[i], compute_services_[i], pd_, cq_,
                        av_, FI_ADDR_UNSPEC);
    conn_.at(i) = std::move(connection);
}

std::string InputChannelSender::get_state_string()
{
    std::ostringstream s;

    s << "/--- desc buf ---" << std::endl;
    s << "|";
    for (unsigned int i = 0; i < data_source_.desc_buffer().size(); ++i)
        s << " (" << i << ")" << data_source_.desc_buffer().at(i).offset;
    s << std::endl;
    s << "| acked_desc_ = " << acked_desc_ << std::endl;
    s << "/--- data buf ---" << std::endl;
    s << "|";
    for (unsigned int i = 0; i < data_source_.data_buffer().size(); ++i)
        s << " (" << i << ")" << std::hex << data_source_.data_buffer().at(i)
          << std::dec;
    s << std::endl;
    s << "| acked_data_ = " << acked_data_ << std::endl;
    s << "\\---------";

    return s.str();
}

void InputChannelSender::post_send_data(uint64_t timeslice, int cn,
                                        uint64_t desc_offset,
                                        uint64_t desc_length,
                                        uint64_t data_offset,
                                        uint64_t data_length, uint64_t skip)
{
    int num_sge = 0;
    struct iovec sge[4];
    void* descs[4];
    // descriptors
    if ((desc_offset & data_source_.desc_send_buffer().size_mask()) <=
        ((desc_offset + desc_length - 1) &
         data_source_.desc_send_buffer().size_mask())) {
        // one chunk
        sge[num_sge].iov_base =
            &data_source_.desc_send_buffer().at(desc_offset);
        sge[num_sge].iov_len = sizeof(fles::MicrosliceDescriptor) * desc_length;
        assert(mr_desc_ != nullptr);
        descs[num_sge++] = fi_mr_desc(mr_desc_);
        // sge[num_sge++].lkey = mr_desc_->lkey;
    } else {
        // two chunks
        sge[num_sge].iov_base =
            &data_source_.desc_send_buffer().at(desc_offset);
        sge[num_sge].iov_len =
            sizeof(fles::MicrosliceDescriptor) *
            (data_source_.desc_send_buffer().size() -
             (desc_offset & data_source_.desc_send_buffer().size_mask()));
        descs[num_sge++] = fi_mr_desc(mr_desc_);
        sge[num_sge].iov_base = data_source_.desc_send_buffer().ptr();
        sge[num_sge].iov_len =
            sizeof(fles::MicrosliceDescriptor) *
            (desc_length - data_source_.desc_send_buffer().size() +
             (desc_offset & data_source_.desc_send_buffer().size_mask()));
        descs[num_sge++] = fi_mr_desc(mr_desc_);
    }
    int num_desc_sge = num_sge;
    // data
    if (data_length == 0) {
        // zero chunks
    } else if ((data_offset & data_source_.data_send_buffer().size_mask()) <=
               ((data_offset + data_length - 1) &
                data_source_.data_send_buffer().size_mask())) {
        // one chunk
        sge[num_sge].iov_base =
            &data_source_.data_send_buffer().at(data_offset);
        sge[num_sge].iov_len = data_length;
        descs[num_sge++] = fi_mr_desc(mr_data_);
    } else {
        // two chunks
        sge[num_sge].iov_base =
            &data_source_.data_send_buffer().at(data_offset);
        sge[num_sge].iov_len =
            data_source_.data_send_buffer().size() -
            (data_offset & data_source_.data_send_buffer().size_mask());
        descs[num_sge++] = fi_mr_desc(mr_data_);
        sge[num_sge].iov_base = data_source_.data_send_buffer().ptr();
        sge[num_sge].iov_len =
            data_length - data_source_.data_send_buffer().size() +
            (data_offset & data_source_.data_send_buffer().size_mask());
        descs[num_sge++] = fi_mr_desc(mr_data_);
    }
    // copy between buffers
    for (int i = 0; i < num_sge; ++i) {
        if (i < num_desc_sge) {
            data_source_.copy_to_desc_send_buffer(
                reinterpret_cast<fles::MicrosliceDescriptor*>(sge[i].iov_base) -
                    data_source_.desc_send_buffer().ptr(),
                sge[i].iov_len / sizeof(fles::MicrosliceDescriptor));
        } else {
            data_source_.copy_to_data_send_buffer(
                reinterpret_cast<uint8_t*>(sge[i].iov_base) -
                    data_source_.data_send_buffer().ptr(),
                sge[i].iov_len);
        }
    }

    conn_[cn]->send_data(sge, descs, num_sge, timeslice, desc_length,
                         data_length, skip);
}

uint64_t InputChannelSender::get_interval_index(uint64_t timeslice)
{
    uint64_t interval_index = intervals_info_.get_last_key();
    InputIntervalInfo* interval_info = intervals_info_.get(interval_index);
    while (interval_index >= 0 && timeslice < interval_info->start_ts){
	interval_info = intervals_info_.get(--interval_index);
    }
    return interval_index;
}

void InputChannelSender::on_completion(uint64_t wr_id)
{
    switch (wr_id & 0xFF) {
    case ID_WRITE_DESC: {
        uint64_t ts = wr_id >> 24;

        int cn = (wr_id >> 8) & 0xFFFF;
        conn_[cn]->on_complete_write();
        double duration = std::chrono::duration_cast<std::chrono::microseconds>(
		std::chrono::high_resolution_clock::now() - conn_[cn]->get_sent_time(ts)).count();
        conn_[cn]->add_sent_duration(ts, duration);
        intervals_info_.get(get_interval_index(ts))->count_acked_ts++;

        /// LOGGING
        if (ConstVariables::ENABLE_LOGGING){
            timeslice_duration_log_.insert(std::pair<uint64_t, uint64_t>(ts, duration));
        }/// END LOGGING

        uint64_t acked_ts = (acked_desc_ - start_index_desc_) / timeslice_size_;
        if (ts != acked_ts) {
            // transmission has been reordered, store completion information
            ack_.at(ts) = ts;
        } else {
            // completion is for earliest pending timeslice, update indices
            do {
                ++acked_ts;
            } while (ack_.at(acked_ts) > ts);

            acked_desc_ = acked_ts * timeslice_size_ + start_index_desc_;
	    acked_data_ =
		data_source_.desc_buffer().at(acked_desc_ - 1).offset +
		data_source_.desc_buffer().at(acked_desc_ - 1).size;
	    if (acked_data_ >= cached_acked_data_ + min_acked_data_ ||
		acked_desc_ >= cached_acked_desc_ + min_acked_desc_) {
		cached_acked_data_ = acked_data_;
		cached_acked_desc_ = acked_desc_;
            }
        }
        if (false) {
            L_(trace) << "[i" << input_index_ << "] "
                      << "write timeslice " << ts
                      << " complete, now: acked_data_=" << acked_data_
                      << " acked_desc_=" << acked_desc_;
        }
    } break;

    case ID_RECEIVE_STATUS: {
        int cn = wr_id >> 8;
        conn_[cn]->on_complete_recv();
        if (!connection_oriented_ && !conn_[cn]->get_partner_addr()) {
            conn_[cn]->set_partner_addr(av_);
            conn_[cn]->set_remote_info();
            on_connected(pd_);
            ++connected_;
            connected_indexes_.insert(cn);
        }
        if (conn_[cn]->request_abort_flag()) {
            abort_ = true;
        }
        if (conn_[cn]->done()) {
            ++connections_done_;
            all_done_ = (connections_done_ == conn_.size());
            if (!connection_oriented_) {
                on_disconnected(nullptr, cn);
            }
            L_(debug) << "[i" << input_index_ << "] "
                      << "ID_RECEIVE_STATUS final for id " << cn
                      << " all_done=" << all_done_;
        }
    } break;

    case ID_SEND_STATUS: {
    } break;

    default:
        L_(fatal) << "[i" << input_index_ << "] "
                  << "wc for unknown wr_id=" << (wr_id & 0xFF);
        throw LibfabricException("wc for unknown wr_id");
    }
}
}
