// Copyright 2019 Farouk Salem <salem@zib.de>

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
    // TODO remove
    assert(false);
    interval_scheduler_->update_compute_connection_count(compute_count);
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
    if (interval_scheduler_->add_proposed_meta_data(meta_data)){
	std::vector<uint32_t> dist (meta_data.compute_nodes_distribution, meta_data.compute_nodes_distribution + get_compute_connection_count());
	update_compute_distribution_frequency(meta_data.start_timeslice, meta_data.last_timeslice, dist);
    }
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
    if (is_connection_timed_out(compute_index))return ConstVariables::MINUS_ONE;
    // TODO TO BE UPDATED
    uint64_t next = timeslice_manager_->get_connection_next_timeslice(compute_index);
    if (next == ConstVariables::MINUS_ONE || (timeslice_trigger != ConstVariables::MINUS_ONE && next >= timeslice_trigger))
	return ConstVariables::MINUS_ONE;
    return next;
}

void InputSchedulerOrchestrator::mark_timeslice_transmitted(uint32_t compute_index, uint64_t timeslice, uint64_t size){
    interval_scheduler_->increament_sent_timeslices(timeslice);
    timeslice_manager_->log_timeslice_transmit_time(compute_index, timeslice, size);
    sent_timeslices++;
}

bool InputSchedulerOrchestrator::mark_timeslice_rdma_write_acked(uint32_t compute_index, uint64_t timeslice){
    return timeslice_manager_->acknowledge_timeslice_rdma_write(compute_index, timeslice);
}

void InputSchedulerOrchestrator::mark_timeslices_acked(uint32_t compute_index, uint64_t up_to_descriptor_id){
    uint64_t last_descriptor = timeslice_manager_->get_last_acked_descriptor(compute_index);
    for (uint64_t desc = last_descriptor + 1 ; desc <= up_to_descriptor_id ; ++desc){
	uint64_t timeslice = get_timeslice_by_descriptor(compute_index, desc);
	if (timeslice == ConstVariables::MINUS_ONE){
	    L_(warning) << "Desc " << desc << " in compute conn_" << compute_index << " does not exist in the TimesliceManager database!!!";
	    continue;
	}
	interval_scheduler_->increament_acked_timeslices(timeslice);
	// TODO to be removed or re-written
	InputIntervalInfo* interval = interval_scheduler_->get_interval_of_timeslice(timeslice);
	assert (interval != nullptr);
	uint64_t next_compute_timeslice = get_timeslice_by_descriptor(compute_index, desc+1);
	if (timeslice > 9990 && timeslice < 10050)
	    L_(info) << "C_ " << compute_index << " ts " << timeslice << " next " << next_compute_timeslice;
	if (next_compute_timeslice != ConstVariables::MINUS_ONE && next_compute_timeslice > interval->end_ts){
	    uint64_t count = timeslice_manager_->get_count_timeslices_of_interval(compute_index, interval->start_ts, interval->end_ts),
		    duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - interval->actual_start_time).count();
	    L_(info) << "[I:" << interval->index << "] c_" << compute_index << " sent " << count
		     << " in " << duration
		     << "ms(" << (duration/(count*1.0)) << " #ts/ms)"
		     << " ts " << timeslice << " next " << next_compute_timeslice;
	}
    }

    uint64_t avg_latency = timeslice_manager_->acknowledge_timeslices_completion(compute_index, up_to_descriptor_id);
    if (avg_latency > 0) heartbeat_manager_->log_new_latency(compute_index, avg_latency);
}

bool InputSchedulerOrchestrator::is_timeslice_rdma_acked(uint32_t compute_index, uint64_t timeslice){
    return timeslice_manager_->is_timeslice_rdma_acked(compute_index, timeslice);
}

uint64_t InputSchedulerOrchestrator::get_last_acked_descriptor(uint32_t compute_index){
    return timeslice_manager_->get_last_acked_descriptor(compute_index);
}

uint64_t InputSchedulerOrchestrator::get_timeslice_by_descriptor(uint32_t compute_index, uint64_t descriptor){
    return timeslice_manager_->get_timeslice_by_descriptor(compute_index, descriptor);
}

uint64_t InputSchedulerOrchestrator::get_last_connection_descriptor_index(uint32_t compute_index){
    return timeslice_manager_->get_last_connection_descriptor_index(compute_index);
}

std::pair<uint64_t, uint64_t> InputSchedulerOrchestrator::get_data_and_desc_of_timeslice(uint32_t compute_index, uint32_t timeslice){
    return timeslice_manager_->get_data_and_desc_of_timeslice(compute_index, timeslice);
}

std::pair<uint64_t, uint64_t> InputSchedulerOrchestrator::get_data_and_desc_of_last_timeslice(uint32_t compute_index){
    return timeslice_manager_->get_data_and_desc_of_last_timeslice(compute_index);
}

std::pair<uint64_t, uint64_t> InputSchedulerOrchestrator::get_data_and_desc_of_last_rdma_acked_timeslice(uint32_t compute_index){
    uint64_t timeslice = timeslice_manager_->get_last_rdma_acked_timeslice(compute_index);
    return get_data_and_desc_of_timeslice(compute_index, timeslice);
}

void InputSchedulerOrchestrator::consider_reschedule_decision(HeartbeatFailedNodeInfo failed_node_info){
    if (is_decision_considered(failed_node_info.index))return;

    mark_timeslices_acked(failed_node_info.index, failed_node_info.last_completed_desc);
    L_(debug) << "consider_reschedule_decision of " << failed_node_info.index << " sent " << sent_timeslices;
    std::vector<uint64_t> undo_timeslices = timeslice_manager_->consider_reschedule_decision(failed_node_info, retrieve_timeout_connections());
    interval_scheduler_->undo_increament_sent_timeslices(undo_timeslices);
    sent_timeslices -= undo_timeslices.size();
    L_(debug) << "Undo " << undo_timeslices.size() << " .... new sent_timeslices = " << sent_timeslices;
    // TODO
    InputSchedulerOrchestrator::last_timeslice_trigger = InputSchedulerOrchestrator::timeslice_trigger;
    InputSchedulerOrchestrator::timeslice_trigger = ConstVariables::MINUS_ONE;
    //InputSchedulerOrchestrator::SHOW_LOGS_ = true;
}

bool InputSchedulerOrchestrator::is_decision_considered(uint32_t connection_id){
    return timeslice_manager_->is_decision_considered(connection_id);
}

void InputSchedulerOrchestrator::update_compute_distribution_frequency(uint64_t start_timeslice, uint64_t last_timeslice, std::vector<uint32_t> compute_frequency){
    assert (compute_frequency.size() == get_compute_connection_count());
    std::vector<uint64_t> undo_timeslices = timeslice_manager_->update_compute_distribution_frequency(start_timeslice, last_timeslice, compute_frequency);
    interval_scheduler_->undo_increament_sent_timeslices(undo_timeslices);
    sent_timeslices -= undo_timeslices.size();
}

void InputSchedulerOrchestrator::log_timeslice_IB_blocked(uint32_t compute_index, uint64_t timeslice, bool sent_completed){
    uint64_t duration = timeslice_manager_->log_timeslice_IB_blocked(timeslice, sent_completed);
    if (sent_completed && duration > ConstVariables::ZERO){
    	interval_scheduler_->add_input_buffer_blockage_duration(compute_index, timeslice, duration);
    }
}

void InputSchedulerOrchestrator::log_timeslice_CB_blocked(uint32_t compute_index, uint64_t timeslice, bool sent_completed){
    uint64_t duration = timeslice_manager_->log_timeslice_CB_blocked(timeslice, sent_completed);
    if (sent_completed && duration > ConstVariables::ZERO){
	interval_scheduler_->add_compute_buffer_blockage_duration(compute_index, timeslice, duration);
    }
}

void InputSchedulerOrchestrator::log_timeslice_MR_blocked(uint32_t compute_index, uint64_t timeslice, bool sent_completed){
    timeslice_manager_->log_timeslice_MR_blocked(timeslice, sent_completed);
}

//// InputHeartbeatManager Methods

void InputSchedulerOrchestrator::log_heartbeat(uint32_t connection_id){
    heartbeat_manager_->log_heartbeat(connection_id);
}

std::vector<uint32_t> InputSchedulerOrchestrator::retrieve_new_inactive_connections(){
    return heartbeat_manager_->retrieve_new_inactive_connections();
}

// Retrieve a list of timeout connections
const std::set<uint32_t> InputSchedulerOrchestrator::retrieve_timeout_connections(){
    return heartbeat_manager_->retrieve_timeout_connections();
}

int32_t InputSchedulerOrchestrator::get_new_timeout_connection(){
    return heartbeat_manager_->get_new_timeout_connection();
}

bool InputSchedulerOrchestrator::is_connection_timed_out(uint32_t connection_id){
    return heartbeat_manager_->is_connection_timed_out(connection_id);
}

void InputSchedulerOrchestrator::mark_connection_timed_out(uint32_t connection_id){
    heartbeat_manager_->mark_connection_timed_out(connection_id);
}

void InputSchedulerOrchestrator::log_sent_heartbeat_message(HeartbeatMessage message){
    heartbeat_manager_->log_sent_heartbeat_message(message);
}

uint64_t InputSchedulerOrchestrator::get_next_heartbeat_message_id(){
    return heartbeat_manager_->get_next_heartbeat_message_id();
}

uint32_t InputSchedulerOrchestrator::get_active_connection_count(){
    return heartbeat_manager_->get_active_connection_count();
}

uint32_t InputSchedulerOrchestrator::get_timeout_connection_count(){
    return heartbeat_manager_->get_timeout_connection_count();
}
//// Methods combine data from different objects

HeartbeatFailedNodeInfo InputSchedulerOrchestrator::get_timed_out_connection(int32_t timeout_conn){
    if (timeout_conn == -1) timeout_conn = get_new_timeout_connection();
    else mark_connection_timed_out(timeout_conn);

    HeartbeatFailedNodeInfo failure_info = HeartbeatFailedNodeInfo();
    if (timeout_conn != -1){
	failure_info.index = timeout_conn;
	failure_info.last_completed_desc = get_last_acked_descriptor(timeout_conn);
	// TODO last possible timeslice
	failure_info.timeslice_trigger = get_up_to_timeslice_trigger();
	// TODO info.timeslice_trigger = timeslice_manager_->get_last_timeslice_before_blockage(timeout_conn);
	// TODO stop transmitting TSs until receiving an ACK
	InputSchedulerOrchestrator::timeslice_trigger = failure_info.timeslice_trigger;
	//-----
    }
    return failure_info;
}

//// Variables

InputIntervalScheduler* InputSchedulerOrchestrator::interval_scheduler_ = nullptr;
InputTimesliceManager* InputSchedulerOrchestrator::timeslice_manager_ = nullptr;
InputHeartbeatManager* InputSchedulerOrchestrator::heartbeat_manager_ = nullptr;


// TODO TO BE REMOVED
uint64_t InputSchedulerOrchestrator::timeslice_trigger;
uint64_t InputSchedulerOrchestrator::data_source_desc = ConstVariables::ZERO;
uint32_t InputSchedulerOrchestrator::timeslice_size;
uint32_t InputSchedulerOrchestrator::overlap_size;
uint64_t InputSchedulerOrchestrator::start_index_desc;
uint64_t InputSchedulerOrchestrator::sent_timeslices;
bool InputSchedulerOrchestrator::SHOW_LOGS_ = false;
uint64_t InputSchedulerOrchestrator::last_timeslice_trigger;
bool InputSchedulerOrchestrator::FREQUENCY_UPDATED_ = false;

uint64_t InputSchedulerOrchestrator::get_up_to_timeslice_trigger(){
    uint64_t timeslice = sent_timeslices-2;
    while(1){
	uint64_t desc_offset = (timeslice) * timeslice_size + start_index_desc;
	uint64_t desc_length = timeslice_size + overlap_size;
	// check if microslice no. (desc_offset + desc_length - 1) is avail
	if (data_source_desc >= desc_offset + desc_length) ++timeslice;
	else break;
    }
    --timeslice;
    const std::set<uint32_t> timeout_connections = retrieve_timeout_connections();
    while (!timeout_connections.empty() &&
	    timeslice_manager_->is_timeslice_belongs_to_timeout_connection(timeslice, timeout_connections)){
	--timeslice;
    }

    return timeslice;
}
}
