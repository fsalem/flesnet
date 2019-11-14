// Copyright 2019 Farouk Salem <salem@zib.de>


#include "HeartbeatManager.hpp"

namespace tl_libfabric
{


HeartbeatManager* HeartbeatManager::get_instance(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging){
    if (instance_ == nullptr){
    	instance_ = new HeartbeatManager(index, init_connection_count, log_directory, enable_logging);
    }
    return instance_;
}

HeartbeatManager* HeartbeatManager::get_instance(){
    return instance_;
}

HeartbeatManager::HeartbeatManager(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging):
		index_(index),
		connection_count_(init_connection_count),
		log_directory_(log_directory),
		enable_logging_(enable_logging){
    assert( init_connection_count > 0);
    for (uint32_t conn = 0 ; conn < init_connection_count ; ++conn){
	connection_heartbeat_time_.push_back(std::chrono::high_resolution_clock::now());
    }
    timeout_ = ConstVariables::TIMEOUT;
}


void HeartbeatManager::log_heartbeat(uint32_t connection_id){
    assert (connection_id < connection_heartbeat_time_.size());
    connection_heartbeat_time_[connection_id] = std::chrono::high_resolution_clock::now();
}

std::vector<uint32_t> HeartbeatManager::retrieve_timeout_connections(){
    std::vector<uint32_t> timed_out_conns;
    for (uint32_t i = 0 ; i < connection_heartbeat_time_.size() ; i++){
	if (is_connection_timed_out(i))
	    timed_out_conns.push_back(i);
    }
    return timed_out_conns;
}

bool HeartbeatManager::is_connection_timed_out(uint32_t connection_id){
    assert (connection_id < connection_heartbeat_time_.size());
    if (connection_timed_out_.find(connection_id) != connection_timed_out_.end()) return true;
    double duration = std::chrono::duration_cast<std::chrono::milliseconds>(
	    std::chrono::high_resolution_clock::now() - connection_heartbeat_time_[connection_id]).count();
    //L_(info) << "[HeartbeatManager][" << index_ << "_" << connection_id << "] dur " << duration;
    if (duration > (timeout_*1000.0)){
	connection_timed_out_.insert(connection_id);
	return true;
    }
    return false;
}


HeartbeatManager* HeartbeatManager::instance_ = nullptr;
}
