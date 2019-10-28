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
	last_received_timeslice_.add(input_conn, 0l);
    }
}


void TimesliceManager::update_input_connection_count(uint32_t input_connection_count) {
    assert (input_connection_count > 0);

    // initially filling the gap
    for (uint32_t input_conn = input_connection_count_ ; input_conn < input_connection_count ; ++input_conn){
	last_received_timeslice_.add(input_conn, 0l);
    }
    input_connection_count_ = input_connection_count;
}

void TimesliceManager::log_contribution_arrival(uint32_t connection_id, uint64_t timeslice){
    assert (connection_id < input_connection_count_);
    uint64_t last_received_ts = last_received_timeslice_.get(connection_id);
    assert (last_received_ts <= timeslice);
    if (last_received_ts == timeslice) return;

    for (uint64_t ts = last_received_ts+1 ; ts <= timeslice ; ++ts){
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
    last_received_timeslice_.update(connection_id, timeslice);

}

uint64_t TimesliceManager::get_last_ordered_completed_timeslice(){
    if (timeslice_first_arrival_time_.empty() && timeslice_completion_duration_.empty())return 0l;
    if (timeslice_first_arrival_time_.empty())return timeslice_completion_duration_.get_last_key();

    return timeslice_first_arrival_time_.get_begin_iterator()->first - 1;
}

void TimesliceManager::trigger_timeslice_completion (uint64_t timeslice){
    timeslice_completion_duration_.add(timeslice,
		std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::high_resolution_clock::now() - timeslice_first_arrival_time_.get(timeslice)).count());

    timeslice_first_arrival_time_.remove(timeslice);
    timeslice_arrived_count_.remove(timeslice);
}

void TimesliceManager::generate_log_files(){
    if (!enable_logging_) return;

    std::ofstream log_file;
    log_file.open(log_directory_+"/"+std::to_string(compute_index_)+".compute.arrival_diff.out");

    log_file << std::setw(25) << "Timeslice" << std::setw(25) << "Diff" << "\n";
    SizedMap<uint64_t,double>::iterator it = timeslice_completion_duration_.get_begin_iterator();
    while(it != timeslice_completion_duration_.get_end_iterator()){
	log_file << std::setw(25) << it->first << std::setw(25) << it->second << "\n";
	++it;
    }

    log_file.flush();
    log_file.close();
}

TimesliceManager* TimesliceManager::instance_ = nullptr;
}
