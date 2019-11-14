// Copyright 2018 Farouk Salem <salem@zib.de>

#include "InputTimesliceManager.hpp"

// TO BE REMOVED
#include <fstream>
#include <iomanip>

namespace tl_libfabric
{
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
    return instance_;
}

void InputTimesliceManager::update_compute_connection_count(uint32_t compute_count){
    compute_count_ = compute_count;
}

void InputTimesliceManager::update_input_scheduler_index(uint32_t scheduler_index){
    scheduler_index_ = scheduler_index;
}

// TODO INTERVAL
void InputTimesliceManager::increament_sent_timeslices(){
}

void InputTimesliceManager::increament_acked_timeslices(uint64_t timeslice){
}

uint32_t InputTimesliceManager::get_compute_connection_count(){
    return compute_count_;
}

// TODO TIMESLICE
bool InputTimesliceManager::is_timeslice_acked(uint64_t timeslice){
    // If timeslice transmit time is not logged, assume it is acked
    if (!timeslice_info_log_.contains(timeslice))
	return true;
    TimesliceInfo* timeslice_info = timeslice_info_log_.get(timeslice);
    return timeslice_info->acked_duration == 0 ? false : true;
}

// PRIVATE
InputTimesliceManager::InputTimesliceManager(uint32_t scheduler_index, uint32_t compute_conn_count,
	uint32_t interval_length, std::string log_directory, bool enable_logging):
    		scheduler_index_(scheduler_index), compute_count_(compute_conn_count),
		interval_length_(interval_length), log_directory_(log_directory),
		enable_logging_(enable_logging) {
}

InputTimesliceManager* InputTimesliceManager::instance_ = nullptr;

void InputTimesliceManager::log_timeslice_ack_time(uint64_t timeslice){
    if (!timeslice_info_log_.contains(timeslice))return;
    TimesliceInfo* timeslice_info = timeslice_info_log_.get(timeslice);
    timeslice_info->acked_duration = std::chrono::duration_cast<std::chrono::microseconds>(
	    std::chrono::high_resolution_clock::now() - timeslice_info->transmit_time).count();
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

}
