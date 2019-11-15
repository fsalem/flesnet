// Copyright 2018 Farouk Salem <salem@zib.de>

#include "InputTimesliceManager.hpp"

// TO BE REMOVED
#include <fstream>
#include <iomanip>

namespace tl_libfabric
{

InputTimesliceManager::InputTimesliceManager(uint32_t scheduler_index, uint32_t compute_conn_count,
	uint32_t interval_length, std::string log_directory, bool enable_logging):
    		scheduler_index_(scheduler_index), compute_count_(compute_conn_count),
		interval_length_(interval_length), log_directory_(log_directory),
		enable_logging_(enable_logging) {
    for (uint32_t i = 0 ; i< compute_count_ ; ++i){
	conn_timeslice_info_.add(i, new SizedMap<uint64_t, TimesliceInfo*>());
	conn_desc_timeslice_info_.add(i, new SizedMap<uint64_t, uint64_t>());
    }
}

// PUBLIC
InputTimesliceManager* InputTimesliceManager::get_instance(uint32_t scheduler_index, uint32_t compute_conn_count,
					    uint32_t interval_length,
					    std::string log_directory, bool enable_logging){
    if (instance_ == nullptr){
	instance_ = new InputTimesliceManager(scheduler_index, compute_conn_count, interval_length, log_directory, enable_logging);
    }
    return instance_;
}

InputTimesliceManager* InputTimesliceManager::get_instance(){
    assert(instance_ != nullptr);
    return instance_;
}

uint64_t InputTimesliceManager::get_connection_next_timeslice(uint32_t compute_index){
    if (conn_timeslice_info_.get(compute_index)->empty()){
	return compute_index;
    }
    return conn_timeslice_info_.get(compute_index)->get_last_key() + compute_count_;
}

void InputTimesliceManager::log_timeslice_transmit_time(uint32_t compute_index, uint64_t timeslice){
    assert (!conn_timeslice_info_.get(compute_index)->contains(timeslice));

    uint64_t descriptor_index = 1;
    if (!conn_timeslice_info_.get(compute_index)->empty()){
	descriptor_index = conn_timeslice_info_.get(compute_index)->
			    get(conn_timeslice_info_.get(compute_index)->get_last_key())->compute_desc + 1;
    }
    
    TimesliceInfo* timeslice_info = new TimesliceInfo();
    timeslice_info->compute_desc = descriptor_index;
    timeslice_info->transmit_time = std::chrono::high_resolution_clock::now();
    assert(conn_timeslice_info_.get(compute_index)->add(timeslice, timeslice_info));
    assert(conn_desc_timeslice_info_.get(compute_index)->add(descriptor_index, timeslice));
}

void InputTimesliceManager::acknowledge_timeslice_rdma_write(uint32_t compute_index, uint64_t timeslice){
    if (!conn_timeslice_info_.get(compute_index)->contains(timeslice)){
    	L_(warning) << "[i_" << scheduler_index_ << "][ACK_RDMA_WRITE] ts " << timeslice << " does not belong to conn_" << compute_index;
    	return;
    }
    TimesliceInfo* timeslice_info = conn_timeslice_info_.get(compute_index)->get(timeslice);
    timeslice_info->rdma_acked_duration = std::chrono::duration_cast<std::chrono::microseconds>(
	    		std::chrono::high_resolution_clock::now() - timeslice_info->transmit_time).count();
}

void InputTimesliceManager::acknowledge_timeslices_completion(uint32_t compute_index, uint64_t up_to_descriptor_id){
    SizedMap<uint64_t, uint64_t>::iterator transmitted_timeslice_iterator = conn_desc_timeslice_info_.get(compute_index)->get_begin_iterator();

    while (transmitted_timeslice_iterator != conn_desc_timeslice_info_.get(compute_index)->get_end_iterator()
	    && transmitted_timeslice_iterator->first <= up_to_descriptor_id){

	TimesliceInfo* timeslice_info = conn_timeslice_info_.get(compute_index)->get(transmitted_timeslice_iterator->second);
	timeslice_info->completion_acked_duration = std::chrono::duration_cast<std::chrono::microseconds>(
		std::chrono::high_resolution_clock::now() - timeslice_info->transmit_time).count();

	assert(conn_desc_timeslice_info_.get(compute_index)->remove(transmitted_timeslice_iterator->first));
	transmitted_timeslice_iterator = conn_desc_timeslice_info_.get(compute_index)->get_begin_iterator();
    }
}

bool InputTimesliceManager::is_timeslice_rdma_acked(uint32_t compute_index, uint64_t timeslice){
    if (!conn_timeslice_info_.get(compute_index)->contains(timeslice)){
	L_(warning) << "[i_" << scheduler_index_ << "] [RDMA ACKED] ts " << timeslice << " does not belong to conn_" << compute_index;
	return true;
    }

    TimesliceInfo* timeslice_info = conn_timeslice_info_.get(compute_index)->get(timeslice);
    return timeslice_info->rdma_acked_duration == 0 ? false : true;
}

uint32_t InputTimesliceManager::get_compute_connection_count(){
    return compute_count_;
}

uint64_t InputTimesliceManager::get_last_acked_descriptor(uint32_t compute_index){
    if (conn_desc_timeslice_info_.get(compute_index)->empty()){
	if (conn_timeslice_info_.get(compute_index)->empty())return 0;
	return conn_timeslice_info_.get(compute_index)->get(conn_timeslice_info_.get(compute_index)->get_last_key())->compute_desc;
    }
    return conn_desc_timeslice_info_.get(compute_index)->get_begin_iterator()->first - 1;

}

uint64_t InputTimesliceManager::get_timeslice_of_not_acked_descriptor(uint32_t compute_index, uint64_t descriptor){
    return conn_desc_timeslice_info_.get(compute_index)->contains(descriptor) ?
	    conn_desc_timeslice_info_.get(compute_index)->get(descriptor) : ConstVariables::MINUS_ONE;
}

void InputTimesliceManager::generate_log_files(){
    if (!enable_logging_) return;

/////////////////////////////////////////////////////////////////
    std::ofstream blocked_duration_log_file;
    blocked_duration_log_file.open(log_directory_+"/"+std::to_string(scheduler_index_)+".input.ts_blocked_duration.out");

    blocked_duration_log_file << std::setw(25) << "Timeslice" <<
	std::setw(25) << "Compute Index" <<
	std::setw(25) << "IB" <<
	std::setw(25) << "CB" <<
	std::setw(25) << "MR" <<"\n";

    // TODO FIX: it was interval_info_.get(interval_info_.get_last_key())->end_ts
    uint64_t last_ts = 0;
    for (uint64_t ts = 0 ; ts <= last_ts ; ts++){
	blocked_duration_log_file << std::setw(25) << ts <<
	    std::setw(25) << (ts % compute_count_) <<
	    std::setw(25) << (timeslice_IB_blocked_duration_log_.contains(ts) ? timeslice_IB_blocked_duration_log_.get(ts)/1000.0 : 0) <<
	    std::setw(25) << (timeslice_CB_blocked_duration_log_.contains(ts) ? timeslice_CB_blocked_duration_log_.get(ts)/1000.0 : 0) <<
	    std::setw(25) << (timeslice_MR_blocked_duration_log_.contains(ts) ? timeslice_MR_blocked_duration_log_.get(ts)/1000.0 : 0) << "\n";
    }

    blocked_duration_log_file.flush();
    blocked_duration_log_file.close();


    /////////////////////////////////////////////////////////////////

    std::ofstream block_log_file;
    block_log_file.open(log_directory_+"/"+std::to_string(scheduler_index_)+".input.ts_info.out");

    block_log_file << std::setw(25) << "Timeslice"
	<< std::setw(25) << "Compute Index"
	<< std::setw(25) << "RDMA ACK duration"
	<< std::setw(25) << "Completion ACK duration" << "\n";

    for (uint32_t i = 0 ; i < conn_timeslice_info_.size() ; ++i){
	SizedMap<uint64_t, TimesliceInfo*>::iterator delaying_time = conn_timeslice_info_.get(i)->get_begin_iterator();
	while (delaying_time != conn_timeslice_info_.get(i)->get_end_iterator()){
	block_log_file << std::setw(25) << delaying_time->first
		<< std::setw(25) << i
		<< std::setw(25) << delaying_time->second->rdma_acked_duration
		<< std::setw(25) << delaying_time->second->completion_acked_duration
		<< "\n";
	delaying_time++;
	}
    }
    block_log_file.flush();
    block_log_file.close();
}

void InputTimesliceManager::log_timeslice_IB_blocked(uint64_t timeslice, bool sent_completed){
    if (sent_completed){
	if (timeslice_IB_blocked_start_log_.contains(timeslice)){
	    timeslice_IB_blocked_duration_log_.add(timeslice, std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::high_resolution_clock::now() - timeslice_IB_blocked_start_log_.get(timeslice)).count());
	    timeslice_IB_blocked_start_log_.remove(timeslice);
	}
    }else{
	timeslice_IB_blocked_start_log_.add(timeslice, std::chrono::high_resolution_clock::now());
    }
}

void InputTimesliceManager::log_timeslice_CB_blocked(uint64_t timeslice, bool sent_completed){
    if (sent_completed){
	if (timeslice_CB_blocked_start_log_.contains(timeslice)){
	    timeslice_CB_blocked_duration_log_.add(timeslice, std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::high_resolution_clock::now() - timeslice_CB_blocked_start_log_.get(timeslice)).count());
	    timeslice_CB_blocked_start_log_.remove(timeslice);
	}
    }else{
	timeslice_CB_blocked_start_log_.add(timeslice, std::chrono::high_resolution_clock::now());
    }
}

void InputTimesliceManager::log_timeslice_MR_blocked(uint64_t timeslice, bool sent_completed){
    if (sent_completed){
	if (timeslice_MR_blocked_start_log_.contains(timeslice)){
	    timeslice_MR_blocked_duration_log_.add(timeslice, std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::high_resolution_clock::now() - timeslice_MR_blocked_start_log_.get(timeslice)).count());
	    timeslice_MR_blocked_start_log_.remove(timeslice);
	}
    }else{
	timeslice_MR_blocked_start_log_.add(timeslice, std::chrono::high_resolution_clock::now());
    }
}


InputTimesliceManager* InputTimesliceManager::instance_ = nullptr;
}
