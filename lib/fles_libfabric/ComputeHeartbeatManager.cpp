// Copyright 2019 Farouk Salem <salem@zib.de>


#include "ComputeHeartbeatManager.hpp"

namespace tl_libfabric
{

ComputeHeartbeatManager::ComputeHeartbeatManager(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging):
		HeartbeatManager(index, init_connection_count, log_directory, enable_logging){
}

void ComputeHeartbeatManager::calculate_failure_decision(uint32_t failed_node){
    std::vector<FailureRequestedInfo*> collected_info = (*collected_failure_info_.get(failed_node));
    HeartbeatFailedNodeInfo* decision_info = new HeartbeatFailedNodeInfo();
    decision_info->index = failed_node;
    decision_info->last_completed_desc = collected_info[0]->failure_info->last_completed_desc;
    decision_info->timeslice_trigger = collected_info[0]->failure_info->timeslice_trigger;
    for (uint32_t i=1 ; i < connection_count_ ; i++){
	// select max desc
	L_(info) << "cur desc " << decision_info->last_completed_desc << " [" << i << "] " << collected_info[i]->failure_info->last_completed_desc;
	if (decision_info->last_completed_desc < collected_info[i]->failure_info->last_completed_desc)
	    decision_info->last_completed_desc = collected_info[i]->failure_info->last_completed_desc;
	// select min timeslice trigger
	L_(info) << "cur timeslice " << decision_info->timeslice_trigger << " [" << i << "] " << collected_info[i]->failure_info->timeslice_trigger;
	if (decision_info->timeslice_trigger > collected_info[i]->failure_info->timeslice_trigger)
	    decision_info->timeslice_trigger = collected_info[i]->failure_info->timeslice_trigger;
    }
    L_(info) << "Decision of " << decision_info->index << " desc " << decision_info->last_completed_desc << " trigger " << decision_info->timeslice_trigger;
    completed_decisions_log_.add(decision_info->index ,decision_info);
    decision_ack_log_.add(decision_info->index, new std::set<uint32_t>());
}

ComputeHeartbeatManager* ComputeHeartbeatManager::get_instance(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging){
    if (instance_ == nullptr){
    	instance_ = new ComputeHeartbeatManager(index, init_connection_count, log_directory, enable_logging);
    }
    return instance_;
}

ComputeHeartbeatManager* ComputeHeartbeatManager::get_instance(){
    return instance_;
}

HeartbeatFailedNodeInfo* ComputeHeartbeatManager::log_heartbeat_failure(uint32_t connection_id, HeartbeatFailedNodeInfo failure_info){
    if (completed_decisions_log_.contains(failure_info.index)){
	L_(warning) << "[" << index_ << "] heartbeat_failure from " << connection_id << " is received after taking the decision!!!";
	return completed_decisions_log_.get(failure_info.index);
    }
    if (!collected_failure_info_.contains(failure_info.index)){
	collected_failure_info_.add(failure_info.index,new std::vector<FailureRequestedInfo*>(connection_count_, nullptr));
	collected_decisions_count_.add(failure_info.index, 0);
    }
    std::vector<FailureRequestedInfo*>* collected_info = collected_failure_info_.get(failure_info.index);
    if ((*collected_info)[connection_id] == nullptr)(*collected_info)[connection_id] = new FailureRequestedInfo();
    if ((*collected_info)[connection_id]->failure_info != nullptr)return nullptr;
    (*collected_info)[connection_id]->failure_info = new HeartbeatFailedNodeInfo(failure_info.index, failure_info.last_completed_desc, failure_info.timeslice_trigger);
    (*collected_info)[connection_id]->info_requested = true;
    uint32_t collected_so_far = collected_decisions_count_.get(failure_info.index) + 1;
    if (collected_so_far == connection_count_){
	collected_decisions_count_.remove(failure_info.index);
	calculate_failure_decision(failure_info.index);
	collected_failure_info_.remove(failure_info.index);
    }else{
	collected_decisions_count_.update(failure_info.index, collected_so_far);
    }
    return nullptr;
}

bool ComputeHeartbeatManager::is_connection_timeout(uint32_t connection_id){
    if (completed_decisions_log_.contains(connection_id) || collected_failure_info_.contains(connection_id)) return true;
    return false;
}

bool ComputeHeartbeatManager::is_decision_ready(uint32_t failed_connection_id){
    return completed_decisions_log_.contains(failed_connection_id) ? true : false;
}

std::pair<uint32_t, std::set<uint32_t>> ComputeHeartbeatManager::retrieve_missing_info_from_connections(){
    // <failed node, list of connections>
    std::pair<uint32_t, std::set<uint32_t>> failed_node_pending_connections;
    failed_node_pending_connections.first = ConstVariables::MINUS_ONE;
    if (!collected_failure_info_.empty()){
	std::vector<FailureRequestedInfo*> failure_data = (*collected_failure_info_.get_begin_iterator()->second);
	for (int i=0 ; i < failure_data.size() ; i++){
	    if (failure_data[i] == nullptr)failure_data[i] = new FailureRequestedInfo();
	    if (!failure_data[i]->info_requested){
		failed_node_pending_connections.first = collected_failure_info_.get_begin_iterator()->first;
		failed_node_pending_connections.second.insert(i);
		failure_data[i]->info_requested = true;
	    }
	}
    }

    return failed_node_pending_connections;
}

HeartbeatFailedNodeInfo* ComputeHeartbeatManager::get_decision_of_failed_connection(uint32_t failed_connection_id){
    if (completed_decisions_log_.contains(failed_connection_id))
	return completed_decisions_log_.get(failed_connection_id);
    return nullptr;
}

void ComputeHeartbeatManager::log_decision_ack(uint32_t connection_id, uint32_t failed_connection_id){
    if (decision_ack_log_.contains(failed_connection_id)){
	// If already a completed decision is created, then this ack is redundant after deleting the entry of decision_ack_log_
	if (completed_decisions_log_.contains(failed_connection_id)){
	    L_(warning) << "[" << index_ << "] log_decision_ack from " << connection_id << " is received completing and deleting the decision_ack_log";
	    return;
	}else{ // Early decision ack is received from creating the decision on this local compute node
	    decision_ack_log_.add(failed_connection_id, new std::set<uint32_t>());
	}
    }

    std::set<uint32_t>* ack_list = decision_ack_log_.get(failed_connection_id);
    if (ack_list->find(connection_id) == ack_list->end())
	ack_list->insert(connection_id);
    if (ack_list->size() == connection_count_)
	decision_ack_log_.remove(failed_connection_id);

}

void ComputeHeartbeatManager::log_finalize_connection(uint32_t connection_id, bool ack_received){
    if (finalize_connection_log_.contains(connection_id)){
	if (!ack_received)return;
	FinalizeConnectionInfo* conn_info = finalize_connection_log_.get(connection_id);
	conn_info->ack_received = ack_received;
    }else{
	FinalizeConnectionInfo* conn_info = new FinalizeConnectionInfo();
	conn_info->sent_time = std::chrono::high_resolution_clock::now();
	finalize_connection_log_.add(connection_id, conn_info);
    }
}

std::vector<uint32_t> ComputeHeartbeatManager::retrieve_long_waiting_finalized_connections(){
    std::vector<uint32_t> conns;
    SizedMap<uint32_t, FinalizeConnectionInfo*>::iterator it = finalize_connection_log_.get_begin_iterator();
    while (it != finalize_connection_log_.get_end_iterator()){
	if (!it->second->ack_received){
	    double duration = std::chrono::duration_cast<std::chrono::milliseconds>(
	    	    std::chrono::high_resolution_clock::now() - it->second->sent_time).count();
	    if (duration >= (ConstVariables::HEARTBEAT_TIMEOUT*1000.0))
		conns.push_back(it->first);
	}
	++it;
    }
    return conns;
}

ComputeHeartbeatManager* ComputeHeartbeatManager::instance_ = nullptr;
}
