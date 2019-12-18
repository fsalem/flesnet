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
    bool decision_ack_received = (heartbeat_manager_->get_decision_ack_size() == ConstVariables::ZERO ? true : false);
    timeslice_manager_->log_contribution_arrival(connection_id, timeslice, decision_ack_received);
    // TODO TO BE REMOVED
    uint64_t last_decision = heartbeat_manager_->get_last_timeslice_decision();
    //if (last_decision != ConstVariables::MINUS_ONE && last_decision + 1000 < timeslice)
	//SHOW_LOG_ = false;
}

bool DDSchedulerOrchestrator::undo_log_contribution_arrival(uint32_t connection_id, uint64_t timeslice){
    return timeslice_manager_->undo_log_contribution_arrival(connection_id, timeslice);
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

//// ComputeHeartbeatManager
void DDSchedulerOrchestrator::log_heartbeat_failure(uint32_t connection_id, HeartbeatFailedNodeInfo failure_info){
    heartbeat_manager_->log_heartbeat_failure(connection_id, failure_info);
    // TODO SHOW_LOG_ = true;
}

std::pair<uint32_t, std::set<uint32_t>> DDSchedulerOrchestrator::retrieve_missing_info_from_connections(){
    return heartbeat_manager_->retrieve_missing_info_from_connections();
}

HeartbeatFailedNodeInfo* DDSchedulerOrchestrator::get_decision_to_broadcast(){
    return heartbeat_manager_->get_decision_to_broadcast();
}

// Log the acknowledge of receiving a decision
void DDSchedulerOrchestrator::log_decision_ack(uint32_t connection_id){
    heartbeat_manager_->log_decision_ack(connection_id);
    if (heartbeat_manager_->get_decision_ack_size() == ConstVariables::ZERO){
	timeslice_manager_->update_timeslice_completion();
    }
}

void DDSchedulerOrchestrator::log_finalize_connection(uint32_t connection_id, bool ack_received){
    heartbeat_manager_->log_finalize_connection(connection_id, ack_received);
}

std::vector<uint32_t> DDSchedulerOrchestrator::retrieve_long_waiting_finalized_connections(){
    return heartbeat_manager_->retrieve_long_waiting_finalized_connections();
}

//// Variables

DDScheduler* DDSchedulerOrchestrator::interval_scheduler_ = nullptr;
ComputeTimesliceManager* DDSchedulerOrchestrator::timeslice_manager_ = nullptr;
ComputeHeartbeatManager* DDSchedulerOrchestrator::heartbeat_manager_ = nullptr;
bool DDSchedulerOrchestrator::SHOW_LOG_ = false;
}
