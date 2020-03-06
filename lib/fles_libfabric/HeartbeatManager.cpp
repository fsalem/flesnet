// Copyright 2019 Farouk Salem <salem@zib.de>


#include "HeartbeatManager.hpp"

namespace tl_libfabric
{


HeartbeatManager::HeartbeatManager(uint32_t index, uint32_t init_connection_count,
	    std::string log_directory, bool enable_logging):
		index_(index),
		connection_count_(init_connection_count),
		log_directory_(log_directory),
		enable_logging_(enable_logging){
    assert( init_connection_count > 0);
}

void HeartbeatManager::log_sent_heartbeat_message(uint32_t connection_id, HeartbeatMessage message){
    HeartbeatMessageInfo* message_info = new HeartbeatMessageInfo();
    message_info->message = message;
    message_info->transmit_time = std::chrono::high_resolution_clock::now();
    message_info->dest_connection = connection_id;
    heartbeat_message_log_.insert(message_info);
}

uint64_t HeartbeatManager::get_next_heartbeat_message_id(){
    return heartbeat_message_log_.size()+1;
    // TODO consider cleaning the set to prevent buffer overflow over time
    /* This code does not work correctly because it could return the same id
    if (heartbeat_message_log_.empty())return ConstVariables::ZERO;
    return (*(--heartbeat_message_log_.end()))->message.message_id+1;
    */
}

}
