// Copyright 2019 Farouk Salem <salem@zib.de>


#include "InputHeartbeatManager.hpp"

namespace tl_libfabric
{


InputHeartbeatManager* InputHeartbeatManager::get_instance(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging){
    if (instance_ == nullptr){
    	instance_ = new InputHeartbeatManager(index, init_connection_count, log_directory, enable_logging);
    }
    return instance_;
}

InputHeartbeatManager* InputHeartbeatManager::get_instance(){
    return instance_;
}

InputHeartbeatManager::InputHeartbeatManager(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging):
		index_(index),
		connection_count_(init_connection_count),
		log_directory_(log_directory),
		enable_logging_(enable_logging){
    assert( init_connection_count > 0);
    uint64_t timeout = ConstVariables::HEARTBEAT_TIMEOUT;
    for (uint32_t conn = 0 ; conn < init_connection_count ; ++conn){
	connection_heartbeat_time_.push_back(new ConnectionHeartbeatInfo());
	connection_heartbeat_time_[conn]->latency_history.resize(ConstVariables::HEARTBEAT_TIMEOUT_HISTORY_SIZE, timeout);
    }
    assert (ConstVariables::HEARTBEAT_INACTIVE_FACTOR < ConstVariables::HEARTBEAT_TIMEOUT_FACTOR);
}


void InputHeartbeatManager::log_heartbeat(uint32_t connection_id){
    assert (connection_id < connection_heartbeat_time_.size());
    if (timed_out_connection_.find(connection_id) != timed_out_connection_.end()){
	L_(warning) << "logging heartbeat of timeout connection " << connection_id;
	return;
    }
    ConnectionHeartbeatInfo* conn_info = connection_heartbeat_time_[connection_id];
    conn_info->last_received_message = std::chrono::high_resolution_clock::now();
    // Remove the inactive entry when heartbeat is received
    std::set<uint32_t>::iterator inactive = inactive_connection_.find(connection_id);
    if (inactive != inactive_connection_.end()){
	inactive_connection_.erase(inactive);
    }
}

void InputHeartbeatManager::log_new_latency(uint32_t connection_id, uint64_t latency){
    assert (connection_id < connection_heartbeat_time_.size());
    if (timed_out_connection_.find(connection_id) != timed_out_connection_.end()){
    	L_(warning) << "logging new latency of timeout connection " << connection_id;
    	return;
    }
    //L_(info) << "Latency of [" << connection_id << "] is " << latency;
    ConnectionHeartbeatInfo* conn_info = connection_heartbeat_time_[connection_id];
    conn_info->sum_latency = conn_info->sum_latency - conn_info->latency_history[conn_info->next_latency_index] + latency;
    conn_info->latency_history[conn_info->next_latency_index] = latency;
    conn_info->next_latency_index = (conn_info->next_latency_index+1)%ConstVariables::HEARTBEAT_TIMEOUT_HISTORY_SIZE;
}

std::vector<uint32_t> InputHeartbeatManager::retrieve_new_inactive_connections(){
    std::vector<uint32_t> conns;
    for (uint32_t i = 0 ; i < connection_heartbeat_time_.size() ; i++){
	if (is_connection_inactive(i) && inactive_connection_.find(i) == inactive_connection_.end()){
	    inactive_connection_.insert(i);
	    conns.push_back(i);
	}
    }
    return conns;
}

const std::set<uint32_t> InputHeartbeatManager::retrieve_timeout_connections(){
    return timed_out_connection_;
}

int32_t InputHeartbeatManager::get_new_timeout_connection(){
    std::set<uint32_t>::iterator conn = inactive_connection_.begin();
    while (conn != inactive_connection_.end()){
	if (is_connection_timed_out(*conn) && timed_out_connection_.find(*conn) == timed_out_connection_.end()){
	    timed_out_connection_.insert(*conn);
	    // remove the entry from inactive_connections
	    inactive_connection_.erase(conn);
	    return *conn;
	}
    }
    return -1;
}

bool InputHeartbeatManager::is_connection_inactive(uint32_t connection_id){
    assert (connection_id < connection_heartbeat_time_.size());
    if (inactive_connection_.find(connection_id) != inactive_connection_.end()) return true;
    if (timed_out_connection_.find(connection_id) != timed_out_connection_.end()) return false;
    double duration = std::chrono::duration_cast<std::chrono::microseconds>(
	    std::chrono::high_resolution_clock::now() - connection_heartbeat_time_[connection_id]->last_received_message).count();
    uint64_t avg_latency = connection_heartbeat_time_[connection_id]->sum_latency/ConstVariables::HEARTBEAT_TIMEOUT_HISTORY_SIZE;

    if (duration >= (avg_latency*ConstVariables::HEARTBEAT_INACTIVE_FACTOR)) return true;
    return false;
}

bool InputHeartbeatManager::is_connection_timed_out(uint32_t connection_id){
    assert (connection_id < connection_heartbeat_time_.size());
    if (timed_out_connection_.find(connection_id) != timed_out_connection_.end()) return true;
    double duration = std::chrono::duration_cast<std::chrono::microseconds>(
	std::chrono::high_resolution_clock::now() - connection_heartbeat_time_[connection_id]->last_received_message).count();
    uint64_t avg_latency = connection_heartbeat_time_[connection_id]->sum_latency/ConstVariables::HEARTBEAT_TIMEOUT_HISTORY_SIZE;

    if (duration >= (avg_latency*ConstVariables::HEARTBEAT_TIMEOUT_FACTOR)) return true;
    return false;
}

void InputHeartbeatManager::mark_connection_timed_out(uint32_t connection_id){
    assert (connection_id < connection_heartbeat_time_.size());
    if (timed_out_connection_.find(connection_id) != timed_out_connection_.end()) return;
    if (inactive_connection_.find(connection_id) != inactive_connection_.end())
	inactive_connection_.erase(inactive_connection_.find(connection_id));

    timed_out_connection_.insert(connection_id);
}

void InputHeartbeatManager::log_sent_heartbeat_message(HeartbeatMessage message){
    heartbeat_message_log_.insert(message);
}

uint64_t InputHeartbeatManager::get_next_heartbeat_message_id(){
    if (heartbeat_message_log_.empty())return ConstVariables::ZERO;
    return (--heartbeat_message_log_.end())->message_id+1;
}

uint32_t InputHeartbeatManager::get_active_connection_count(){
    assert(timed_out_connection_.size() <= connection_count_);
    return connection_count_ - timed_out_connection_.size();
}

uint32_t InputHeartbeatManager::get_timeout_connection_count(){
    assert(timed_out_connection_.size() < connection_count_);
    return timed_out_connection_.size();
}

InputHeartbeatManager* InputHeartbeatManager::instance_ = nullptr;
}
