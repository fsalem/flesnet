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
    for (uint32_t conn = 0 ; conn < init_connection_count ; ++conn){
	connection_heartbeat_time_.push_back(std::chrono::high_resolution_clock::now());
    }
    timeout_ = ConstVariables::HEARTBEAT_TIMEOUT;
}


void InputHeartbeatManager::log_heartbeat(uint32_t connection_id){
    assert (connection_id < connection_heartbeat_time_.size());
    assert (timed_out_connection_.find(connection_id) == timed_out_connection_.end());
    connection_heartbeat_time_[connection_id] = std::chrono::high_resolution_clock::now();
    // Remove the inactive entry when heartbeat is received
    std::set<uint32_t>::iterator inactive = inactive_connection_.find(connection_id);
    if (inactive != inactive_connection_.end()){
	inactive_connection_.erase(inactive);
    }
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

std::vector<uint32_t> InputHeartbeatManager::retrieve_new_timeout_connections(){
    std::vector<uint32_t> timed_out_conns;
    std::set<uint32_t>::iterator conn = inactive_connection_.begin(),
				 prev_conn = inactive_connection_.begin();
    while (conn != inactive_connection_.end()){
	if (is_connection_timed_out(*conn) && timed_out_connection_.find(*conn) == timed_out_connection_.end()){
	    timed_out_connection_.insert(*conn);
	    timed_out_conns.push_back(*conn);
	    // remove the entry from inactive_connections
	    inactive_connection_.erase(conn);
	    conn = prev_conn;
	}
	prev_conn = conn;
	++conn;
    }
    return timed_out_conns;
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
    double duration = std::chrono::duration_cast<std::chrono::milliseconds>(
	    std::chrono::high_resolution_clock::now() - connection_heartbeat_time_[connection_id]).count();
    if (duration >= (timeout_*1000.0)) return true;
    return false;
}

bool InputHeartbeatManager::is_connection_timed_out(uint32_t connection_id){
    assert (connection_id < connection_heartbeat_time_.size());
    if (timed_out_connection_.find(connection_id) != timed_out_connection_.end()) return true;
    double duration = std::chrono::duration_cast<std::chrono::milliseconds>(
	std::chrono::high_resolution_clock::now() - connection_heartbeat_time_[connection_id]).count();
    // Check if the last received heartbeat is 2 times the timeout duration
    if (duration >= (2.0 * timeout_*1000.0)) return true;
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


InputHeartbeatManager* InputHeartbeatManager::instance_ = nullptr;
}
