// Copyright 2018 Farouk Salem <salem@zib.de>

#include "InputSchedulerOrchestrator.hpp"


namespace tl_libfabric
{

//// Common Methods
void InputSchedulerOrchestrator::initialize(uint32_t scheduler_index, uint32_t compute_conn_count,
					    uint32_t interval_length,
					    std::string log_directory, bool enable_logging){
    interval_scheduler_ = InputIntervalScheduler::get_instance(scheduler_index, compute_conn_count, interval_length, log_directory, enable_logging);
    timeslice_manager_ = InputTimesliceManager::get_instance(scheduler_index, compute_conn_count, interval_length, log_directory, enable_logging);
    heartbeat_manager_ = InputHeartbeatManager::get_instance(scheduler_index, compute_conn_count, log_directory, enable_logging);
}

void InputSchedulerOrchestrator::update_compute_connection_count(uint32_t compute_count){
    interval_scheduler_->update_compute_connection_count(compute_count);
}

void InputSchedulerOrchestrator::update_input_scheduler_index(uint32_t scheduler_index){
    interval_scheduler_->update_input_scheduler_index(scheduler_index);
}

void InputSchedulerOrchestrator::update_input_begin_time(std::chrono::high_resolution_clock::time_point begin_time){
    interval_scheduler_->update_input_begin_time(begin_time);
}

uint32_t InputSchedulerOrchestrator::get_compute_connection_count(){
    return interval_scheduler_->get_compute_connection_count();
}

void InputSchedulerOrchestrator::generate_log_files(){
    interval_scheduler_->generate_log_files();
    timeslice_manager_->generate_log_files();
}

//// InputIntervalScheduler Methods
void InputSchedulerOrchestrator::add_proposed_meta_data(const IntervalMetaData meta_data){
    interval_scheduler_->add_proposed_meta_data(meta_data);
}

const IntervalMetaData* InputSchedulerOrchestrator::get_actual_meta_data(uint64_t interval_index){
    return interval_scheduler_->get_actual_meta_data(interval_index);
}

uint64_t InputSchedulerOrchestrator::get_last_timeslice_to_send(){
    return interval_scheduler_->get_last_timeslice_to_send();
}

int64_t InputSchedulerOrchestrator::get_next_fire_time(){
    return interval_scheduler_->get_next_fire_time();
}

//// InputTimesliceManager Methods

uint64_t InputSchedulerOrchestrator::get_connection_next_timeslice(uint32_t compute_index){
    return timeslice_manager_->get_connection_next_timeslice(compute_index);
}

void InputSchedulerOrchestrator::mark_timeslice_transmitted(uint32_t compute_index, uint64_t timeslice){
    interval_scheduler_->increament_sent_timeslices();
    timeslice_manager_->log_timeslice_transmit_time(compute_index, timeslice);
}

void InputSchedulerOrchestrator::mark_timeslice_rdma_write_acked(uint32_t compute_index, uint64_t timeslice){
    timeslice_manager_->acknowledge_timeslice_rdma_write(compute_index, timeslice);
}

void InputSchedulerOrchestrator::mark_timeslices_acked(uint32_t compute_index, uint64_t up_to_descriptor_id){
    uint64_t last_descriptor = timeslice_manager_->get_last_acked_descriptor(compute_index);
    for (uint64_t desc = last_descriptor + 1 ; desc <= up_to_descriptor_id ; ++desc){
	uint64_t timeslice = get_timeslice_of_not_acked_descriptor(compute_index, desc);
	if (timeslice == ConstVariables::MINUS_ONE){
	    L_(warning) << "Desc " << desc << " in compute conn_" << compute_index << " does not exist in the TimesliceManager database!!!";
	    continue;
	}
	interval_scheduler_->increament_acked_timeslices(timeslice);
    }

    timeslice_manager_->acknowledge_timeslices_completion(compute_index, up_to_descriptor_id);
}

bool InputSchedulerOrchestrator::is_timeslice_rdma_acked(uint32_t compute_index, uint64_t timeslice){
    return timeslice_manager_->is_timeslice_rdma_acked(compute_index, timeslice);
}

uint64_t InputSchedulerOrchestrator::get_timeslice_of_not_acked_descriptor(uint32_t compute_index, uint64_t descriptor){
    return timeslice_manager_->get_timeslice_of_not_acked_descriptor(compute_index, descriptor);
}

void InputSchedulerOrchestrator::log_timeslice_IB_blocked(uint64_t timeslice, bool sent_completed){
    timeslice_manager_->log_timeslice_IB_blocked(timeslice, sent_completed);
}

void InputSchedulerOrchestrator::log_timeslice_CB_blocked(uint64_t timeslice, bool sent_completed){
    timeslice_manager_->log_timeslice_CB_blocked(timeslice, sent_completed);
}

void InputSchedulerOrchestrator::log_timeslice_MR_blocked(uint64_t timeslice, bool sent_completed){
    timeslice_manager_->log_timeslice_MR_blocked(timeslice, sent_completed);
}

void InputSchedulerOrchestrator::log_heartbeat(uint32_t connection_id){
    heartbeat_manager_->log_heartbeat(connection_id);
}

InputIntervalScheduler* InputSchedulerOrchestrator::interval_scheduler_ = nullptr;
InputTimesliceManager* InputSchedulerOrchestrator::timeslice_manager_ = nullptr;
InputHeartbeatManager* InputSchedulerOrchestrator::heartbeat_manager_ = nullptr;

}
