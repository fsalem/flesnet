// Copyright 2020 Farouk Salem <salem@zib.de>


#include "SchedulerOrchestrator.hpp"

namespace tl_libfabric
{
//// Common Methods

void SchedulerOrchestrator::initialize(HeartbeatManager* heartbeat_manager){
    heartbeat_manager_ = heartbeat_manager;
}


//// HeartbeatManager

void SchedulerOrchestrator::log_sent_heartbeat_message(uint32_t connection_id, HeartbeatMessage message){
    heartbeat_manager_->log_sent_heartbeat_message(connection_id, message);
}

uint64_t SchedulerOrchestrator::get_next_heartbeat_message_id(){
    return heartbeat_manager_->get_next_heartbeat_message_id();
}

void SchedulerOrchestrator::acknowledge_heartbeat_message(uint64_t message_id){
    heartbeat_manager_->ack_message_received(message_id);
}

//// Variables

HeartbeatManager* SchedulerOrchestrator::heartbeat_manager_ = nullptr;
}
