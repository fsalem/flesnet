// Copyright 2019 Farouk Salem <salem@zib.de>


#include "TimesliceManager.hpp"

namespace tl_libfabric
{


TimesliceManager* TimesliceManager::get_instance(uint32_t compute_index,
	uint32_t input_connection_count,
	std::string log_directory,
	bool enable_logging){
    if (instance_ == nullptr){
    	instance_ = new TimesliceManager(compute_index, input_connection_count, log_directory, enable_logging);
    }
    return instance_;
}

TimesliceManager* TimesliceManager::get_instance(){
    return instance_;
}

TimesliceManager::TimesliceManager(uint32_t compute_index,
	uint32_t input_connection_count,
	std::string log_directory, bool enable_logging):
		compute_index_(compute_index),
		input_connection_count_(input_connection_count),
		log_directory_(log_directory),
		enable_logging_(enable_logging){
    assert( input_connection_count > 0);
    for (uint32_t input_conn = 0 ; input_conn < input_connection_count ; ++input_conn){
	last_received_timeslice_.add(input_conn, ConstVariables::MINUS_ONE);
    }
    timeout_ = ConstVariables::TIMEOUT;
}


void TimesliceManager::update_input_connection_count(uint32_t input_connection_count) {
    assert (input_connection_count > 0);

    // initially filling the gap
    for (uint32_t input_conn = input_connection_count_ ; input_conn < input_connection_count ; ++input_conn){
	last_received_timeslice_.add(input_conn, ConstVariables::MINUS_ONE);
    }
    input_connection_count_ = input_connection_count;
}

void TimesliceManager::log_contribution_arrival(uint32_t connection_id, uint64_t timeslice){
    assert (connection_id < input_connection_count_);
    uint64_t last_received_ts = last_received_timeslice_.get(connection_id);
    assert (last_received_ts == ConstVariables::MINUS_ONE || last_received_ts <= timeslice);
    if (timeslice == 0 || (last_received_ts != ConstVariables::MINUS_ONE && last_received_ts+1 == timeslice)) return;

    //L_(info) << "Conn_" << connection_id << " logs up to timeslice " << timeslice << " and last received " <<  last_received_ts;
    for (uint64_t ts = (last_received_ts == ConstVariables::MINUS_ONE ? 0 : last_received_ts+1) ; ts < timeslice ; ++ts){
	if (timeslice_timed_out_.contains(ts))continue;
	if (!timeslice_first_arrival_time_.contains(ts)){
	    timeslice_first_arrival_time_.add(ts, std::chrono::high_resolution_clock::now());
	}

	uint32_t arrived_count = 1;
	if (timeslice_arrived_count_.contains(ts)){
	    arrived_count = timeslice_arrived_count_.get(ts) + 1;
	    timeslice_arrived_count_.update(ts, arrived_count);
	} else {
	    timeslice_arrived_count_.add(ts, arrived_count);
	}

	assert (arrived_count <= input_connection_count_);
	if (arrived_count == input_connection_count_){
	    trigger_timeslice_completion(ts);
	}
    }
    last_received_timeslice_.update(connection_id, timeslice-1);
}

void TimesliceManager::log_timeout_timeslice(){
    SizedMap<uint64_t, std::chrono::high_resolution_clock::time_point>::iterator it;
    double taken_duration;
    uint64_t timeslice;
    while (!timeslice_first_arrival_time_.empty()){
	it = timeslice_first_arrival_time_.get_begin_iterator();
	taken_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::high_resolution_clock::now() - it->second).count();
	if (taken_duration < (timeout_*1000.0))break;
	timeslice =  it->first;

	L_(info) << "----Timeslice " << timeslice << " is timed out after " << taken_duration << " ms (timeout=" << (timeout_*1000.0) << " ms) ---";

	timeslice_timed_out_.add(timeslice, taken_duration);
	assert(timeslice_first_arrival_time_.remove(timeslice));
	assert(timeslice_arrived_count_.remove(timeslice));
    }
}

uint64_t TimesliceManager::get_last_ordered_completed_timeslice(){
    if (timeslice_first_arrival_time_.empty()){
	uint64_t last_completed = (timeslice_completion_duration_.empty() ? ConstVariables::MINUS_ONE: timeslice_completion_duration_.get_last_key()),
		 last_timeout =  (timeslice_timed_out_.empty() ? ConstVariables::MINUS_ONE : timeslice_timed_out_.get_last_key());
	if (last_completed == ConstVariables::MINUS_ONE || last_timeout == ConstVariables::MINUS_ONE)
	    return std::min(last_completed, last_timeout);
	return std::max(last_completed, last_timeout);
    }
    return timeslice_first_arrival_time_.get_begin_iterator()->first == 0 ? ConstVariables::MINUS_ONE : timeslice_first_arrival_time_.get_begin_iterator()->first - 1;
}

bool TimesliceManager::is_timeslice_timed_out(uint64_t timeslice){
    return timeslice_timed_out_.contains(timeslice);
}

void TimesliceManager::trigger_timeslice_completion (uint64_t timeslice){
    if (timeslice_first_arrival_time_.get_begin_iterator()->first != timeslice){
	L_(fatal) << "[trigger_timeslice_completion] first arrival = " << timeslice_first_arrival_time_.get_begin_iterator()->first << ", triggered " << timeslice
		  << ", last completed = " << (timeslice_completion_duration_.empty() ? 0 : timeslice_completion_duration_.get_last_key())
		  << ", last timed out = " << (timeslice_timed_out_.empty() ? 0 : timeslice_timed_out_.get_last_key()) ;
    }
    double duration = std::chrono::duration_cast<std::chrono::microseconds>(
		std::chrono::high_resolution_clock::now() - timeslice_first_arrival_time_.get(timeslice)).count();
    timeslice_completion_duration_.add(timeslice, duration);

    //L_(info) << "----Timeslice " << timeslice << " is COMPLETED after " << duration << " ms";
    // If Timeslice X is completed, then for sure all provious timeslices have to be completed as well
    assert (timeslice_first_arrival_time_.get_begin_iterator()->first == timeslice);
    timeslice_first_arrival_time_.remove(timeslice);
    timeslice_arrived_count_.remove(timeslice);
}

void TimesliceManager::generate_log_files(){
    if (!enable_logging_) return;
    std::ofstream log_file;

    /// Duration to complete each timeslice
    log_file.open(log_directory_+"/"+std::to_string(compute_index_)+".compute.arrival_diff.out");

    log_file << std::setw(25) << "Timeslice" << std::setw(25) << "Diff" << "\n";
    SizedMap<uint64_t,double>::iterator it = timeslice_completion_duration_.get_begin_iterator();
    while(it != timeslice_completion_duration_.get_end_iterator()){
	log_file << std::setw(25) << it->first << std::setw(25) << it->second << "\n";
	++it;
    }

    log_file.flush();
    log_file.close();

    /// Timed out timeslices
    log_file.open(log_directory_+"/"+std::to_string(compute_index_)+".compute.timeout_ts.out");

    log_file << std::setw(25) << "Timeslice" << std::setw(25) << "Duration" << "\n";
    it = timeslice_timed_out_.get_begin_iterator();
    while(it != timeslice_timed_out_.get_end_iterator()){
    log_file << std::setw(25) << it->first << std::setw(25) << it->second << "\n";
    ++it;
    }

    log_file.flush();
    log_file.close();
}

TimesliceManager* TimesliceManager::instance_ = nullptr;
}
