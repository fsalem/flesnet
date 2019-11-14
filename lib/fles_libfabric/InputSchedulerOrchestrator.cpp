// Copyright 2018 Farouk Salem <salem@zib.de>

#include "InputSchedulerOrchestrator.hpp"


namespace tl_libfabric
{
void InputSchedulerOrchestrator::initialize(uint32_t scheduler_index, uint32_t compute_conn_count,
					    uint32_t interval_length,
					    std::string log_directory, bool enable_logging){
    interval_scheduler_ = InputIntervalScheduler::get_instance(scheduler_index, compute_conn_count, interval_length, log_directory, enable_logging);
    timeslice_manager_ = InputTimesliceManager::get_instance(scheduler_index, compute_conn_count, interval_length, log_directory, enable_logging);
    heartbeat_manager_ = HeartbeatManager::get_instance(scheduler_index, compute_conn_count, log_directory, enable_logging);
}

void InputSchedulerOrchestrator::update_compute_connection_count(uint32_t compute_count){
    interval_scheduler_->update_compute_connection_count(compute_count);
    timeslice_manager_->update_compute_connection_count(compute_count);
}

void InputSchedulerOrchestrator::update_input_scheduler_index(uint32_t scheduler_index){
    interval_scheduler_->update_input_scheduler_index(scheduler_index);
    timeslice_manager_->update_input_scheduler_index(scheduler_index);
}

void InputSchedulerOrchestrator::update_input_begin_time(std::chrono::high_resolution_clock::time_point begin_time){
    interval_scheduler_->update_input_begin_time(begin_time);
}

void InputSchedulerOrchestrator::add_proposed_meta_data(const IntervalMetaData meta_data){
    interval_scheduler_->add_proposed_meta_data(meta_data);
}

const IntervalMetaData* InputSchedulerOrchestrator::get_actual_meta_data(uint64_t interval_index){
    return interval_scheduler_->get_actual_meta_data(interval_index);
}

uint64_t InputSchedulerOrchestrator::get_last_timeslice_to_send(){
    return interval_scheduler_->get_last_timeslice_to_send();
}

void InputSchedulerOrchestrator::increament_sent_timeslices(){
    interval_scheduler_->increament_sent_timeslices();
    timeslice_manager_->increament_sent_timeslices();
}

void InputSchedulerOrchestrator::increament_acked_timeslices(uint64_t timeslice){
    interval_scheduler_->increament_acked_timeslices(timeslice);
    timeslice_manager_->increament_acked_timeslices(timeslice);
}

int64_t InputSchedulerOrchestrator::get_next_fire_time(){
    return interval_scheduler_->get_next_fire_time();
}

uint32_t InputSchedulerOrchestrator::get_compute_connection_count(){
    return interval_scheduler_->get_compute_connection_count();
    timeslice_manager_->get_compute_connection_count();
}

bool InputSchedulerOrchestrator::is_timeslice_acked(uint64_t timeslice){
    return timeslice_manager_->is_timeslice_acked(timeslice);
}

void InputSchedulerOrchestrator::log_timeslice_transmit_time(uint64_t timeslice, uint32_t compute_index){
    interval_scheduler_->log_timeslice_transmit_time(timeslice, compute_index);
}

void InputSchedulerOrchestrator::log_timeslice_ack_time(uint64_t timeslice){
    timeslice_manager_->log_timeslice_ack_time(timeslice);
}

void InputSchedulerOrchestrator::generate_log_files(){
    interval_scheduler_->generate_log_files();
    timeslice_manager_->generate_log_files();
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
HeartbeatManager* InputSchedulerOrchestrator::heartbeat_manager_ = nullptr;

}
