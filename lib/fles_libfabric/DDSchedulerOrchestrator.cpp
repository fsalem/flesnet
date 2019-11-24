// Copyright 2018 Farouk Salem <salem@zib.de>


#include "DDSchedulerOrchestrator.hpp"

namespace tl_libfabric
{
//// Common Methods

void DDSchedulerOrchestrator::initialize(uint32_t scheduler_index,
	uint32_t input_scheduler_count,
	uint32_t history_size,
	uint32_t interval_length,
	uint32_t speedup_difference_percentage,
	uint32_t speedup_percentage,
	uint32_t speedup_interval_count,
	std::string log_directory, bool enable_logging){

    interval_scheduler_ = DDScheduler::get_instance(scheduler_index, input_scheduler_count,
	    history_size, interval_length, speedup_difference_percentage,
	    speedup_percentage, speedup_interval_count,
    	    log_directory, enable_logging);
    timeslice_manager_ = ComputeTimesliceManager::get_instance(scheduler_index, input_scheduler_count,
	log_directory, enable_logging);
    heartbeat_manager_ = ComputeHeartbeatManager::get_instance(scheduler_index, input_scheduler_count,
	    log_directory, enable_logging);


}

void DDSchedulerOrchestrator::update_clock_offset(uint32_t input_index, std::chrono::high_resolution_clock::time_point local_time, const uint64_t median_latency, const uint64_t interval_index){
    interval_scheduler_->update_clock_offset(input_index, local_time, median_latency, interval_index);
}

void DDSchedulerOrchestrator::set_begin_time(std::chrono::high_resolution_clock::time_point begin_time) {
    interval_scheduler_->set_begin_time(begin_time);
}

void DDSchedulerOrchestrator::generate_log_files(){
    interval_scheduler_->generate_log_files();
    timeslice_manager_->generate_log_files();
    //heartbeat_manager_->generate_log_files();
}

//// DDScheduler Methods
void DDSchedulerOrchestrator::add_actual_meta_data(uint32_t input_index, IntervalMetaData meta_data) {
    interval_scheduler_->add_actual_meta_data(input_index, meta_data);
}

const IntervalMetaData* DDSchedulerOrchestrator::get_proposed_meta_data(uint32_t input_index, uint64_t interval_index){
    return interval_scheduler_->get_proposed_meta_data(input_index, interval_index);
}

uint64_t DDSchedulerOrchestrator::get_last_completed_interval() {
    return interval_scheduler_->get_last_completed_interval();
}

//// ComputeTimesliceManager Methods

    void DDSchedulerOrchestrator::log_contribution_arrival(uint32_t connection_id, uint64_t timeslice){
	timeslice_manager_->log_contribution_arrival(connection_id, timeslice);
    }

    uint64_t DDSchedulerOrchestrator::get_last_ordered_completed_timeslice(){
	return timeslice_manager_->get_last_ordered_completed_timeslice();
    }

    void DDSchedulerOrchestrator::log_timeout_timeslice(){
	timeslice_manager_->log_timeout_timeslice();
    }

    bool DDSchedulerOrchestrator::is_timeslice_timed_out(uint64_t timeslice){
	return timeslice_manager_->is_timeslice_timed_out(timeslice);
    }

//// Variables

DDScheduler* DDSchedulerOrchestrator::interval_scheduler_ = nullptr;
ComputeTimesliceManager* DDSchedulerOrchestrator::timeslice_manager_ = nullptr;
ComputeHeartbeatManager* DDSchedulerOrchestrator::heartbeat_manager_ = nullptr;
}
