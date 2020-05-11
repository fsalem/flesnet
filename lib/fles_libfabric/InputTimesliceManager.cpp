// Copyright 2018 Farouk Salem <salem@zib.de>

#include "InputTimesliceManager.hpp"

// TO BE REMOVED
#include <fstream>
#include <iomanip>

namespace tl_libfabric
{

InputTimesliceManager::InputTimesliceManager(uint32_t scheduler_index, uint32_t compute_conn_count,
	uint32_t interval_length, std::string log_directory, bool enable_logging):
		compute_count_(compute_conn_count), virtual_compute_count_(compute_conn_count),
		scheduler_index_(scheduler_index), interval_length_(interval_length),
		log_directory_(log_directory), enable_logging_(enable_logging) {

    last_conn_desc_.resize(compute_count_, 0);
    last_conn_timeslice_.resize(compute_count_, ConstVariables::MINUS_ONE);
    virtual_physical_compute_mapping_.resize(compute_count_);
    for (uint32_t i = 0 ; i< compute_count_ ; ++i){
	conn_timeslice_info_.add(i, new SizedMap<uint64_t, TimesliceInfo*>());
	conn_desc_timeslice_info_.add(i, new SizedMap<uint64_t, uint64_t>());
	future_conn_timeslices_.add(i, new std::set<uint64_t>());
	virtual_physical_compute_mapping_[i] = i;
    }
    refill_future_timeslices(interval_length_);
}


void InputTimesliceManager::refill_future_timeslices(uint64_t up_to_timeslice){
    if (next_start_future_timeslice_ >= up_to_timeslice)return;
    uint32_t comp_index = next_start_future_timeslice_%virtual_compute_count_;
    for (uint64_t ts = next_start_future_timeslice_ ; ts < up_to_timeslice ; ++ts){
	while (redistribution_decisions_log_.contains(virtual_physical_compute_mapping_[comp_index]))
	    comp_index = (comp_index+1)%virtual_compute_count_;
	future_conn_timeslices_.get(virtual_physical_compute_mapping_[comp_index])->insert(ts);
	comp_index = (comp_index+1)%virtual_compute_count_;
    }
    next_start_future_timeslice_ = up_to_timeslice;
}

void InputTimesliceManager::check_to_add_rescheduled_timeslices(uint32_t compute_index){
    if (to_be_moved_timeslices_.empty())return;
    // TODO check the correctness of the new desc(s)
    SizedMap<uint64_t, std::vector<std::set<uint64_t>*>>::iterator it = to_be_moved_timeslices_.get_begin_iterator();
    while (it != to_be_moved_timeslices_.get_end_iterator()){
	L_(debug) << "[" << compute_index << "]check to add: trigger " << it->first
	          << " next " << get_connection_next_timeslice(compute_index)
		  << " list " << it->second[compute_index];
	if (get_connection_next_timeslice(compute_index) > it->first &&
		it->second[compute_index] != nullptr && !it->second[compute_index]->empty()){
	    L_(debug) << "adding rescheduled timeslices after " << it->first << " to " << compute_index << " ... last transmitted is " <<
		    conn_timeslice_info_.get(compute_index)->get_last_key();
	    std::set<uint64_t>* rescheduled_timeslices = it->second[compute_index];
	    std::set<uint64_t>::iterator set_it = rescheduled_timeslices->begin();
	    while (set_it != rescheduled_timeslices->end()){
		future_conn_timeslices_.get(compute_index)->insert(*set_it);
		++set_it;
	    }
	    it->second[compute_index] = nullptr;

	}
	++it;
    }
    // TODO to be written in a better way
    while (!to_be_moved_timeslices_.empty()){
	it = to_be_moved_timeslices_.get_begin_iterator();
	bool all_empty = true;
	for (int i=0 ; i<compute_count_ ; i++){
	    if (it->second[i] != nullptr && !it->second[i]->empty()){
		all_empty = false;
		break;
	    }
	}
	if (all_empty){
	    assert(to_be_moved_timeslices_.remove(it->first));
	}else{
	    break;
	}
    }
}

std::vector<uint64_t> InputTimesliceManager::undo_transmitted_timeslices_after_trigger(uint64_t timeslice_trigger){
    std::vector<uint64_t> undo_timeslices;
    if (timeslice_trigger > next_start_future_timeslice_)return undo_timeslices;

    for (uint32_t conn = 0 ; conn < compute_count_ ; ++conn){
	SizedMap<uint64_t, TimesliceInfo*>* timeslices_map = conn_timeslice_info_.get(conn);
	uint64_t last_timeslice;
	TimesliceInfo* last_timeslice_info;
	while (!timeslices_map->empty()){
	    last_timeslice = timeslices_map->get_last_key();
	    if (last_timeslice <= timeslice_trigger)break;
	    last_timeslice_info = timeslices_map->get(last_timeslice);
	    assert(last_timeslice_info->completion_acked_duration == 0);
	    assert (last_timeslice_info->compute_desc == last_conn_desc_[conn]);
	    assert (last_timeslice == last_conn_timeslice_[conn]);
	    // remove conn_desc_timeslices
	    conn_desc_timeslice_info_.get(conn)->remove(last_timeslice_info->compute_desc);
	    // remove conn_timeslice_info
	    timeslices_map->remove(last_timeslice);
	    undo_timeslices.push_back(last_timeslice);
	    L_(debug) << "Removing " << last_timeslice << " from " << conn;
	    // update the last_conn_desc
	    last_conn_desc_[conn]--;
	    // update last_conn_timeslice
	    last_conn_timeslice_[conn] = timeslices_map->get_last_key();
	}
	// remove future_timeslices
	std::set<uint64_t>* future_timeslices = future_conn_timeslices_.get(conn);
	while (!future_timeslices->empty()){
	    last_timeslice = *(--future_timeslices->end());
	    if (last_timeslice <= timeslice_trigger){
		L_(debug) << "last_timeslice of " << conn << " is " << last_timeslice;
		break;
	    }
	    future_timeslices->erase(last_timeslice);
	}
    }
    return undo_timeslices;

}

// PUBLIC
InputTimesliceManager* InputTimesliceManager::get_instance(uint32_t scheduler_index, uint32_t compute_conn_count,
					    uint32_t interval_length,
					    std::string log_directory, bool enable_logging){
    if (instance_ == nullptr){
	instance_ = new InputTimesliceManager(scheduler_index, compute_conn_count, interval_length, log_directory, enable_logging);
    }
    return instance_;
}

InputTimesliceManager* InputTimesliceManager::get_instance(){
    assert(instance_ != nullptr);
    return instance_;
}

uint64_t InputTimesliceManager::get_connection_next_timeslice(uint32_t compute_index){
    if (future_conn_timeslices_.get(compute_index)->empty() &&
    	    std::find(virtual_physical_compute_mapping_.begin(), virtual_physical_compute_mapping_.end(), compute_index)
    		== virtual_physical_compute_mapping_.end()){
	return ConstVariables::MINUS_ONE;
    }
    if (future_conn_timeslices_.get(compute_index)->empty()){
	refill_future_timeslices(next_start_future_timeslice_+interval_length_);
    }
    return (*future_conn_timeslices_.get(compute_index)->begin());
}

void InputTimesliceManager::log_timeslice_transmit_time(uint32_t compute_index, uint64_t timeslice, uint64_t size){
    assert (!conn_timeslice_info_.get(compute_index)->contains(timeslice));

    uint64_t descriptor_index = last_conn_desc_[compute_index] + 1,
             timeslice_data = size +
		 (last_conn_timeslice_[compute_index] == ConstVariables::MINUS_ONE ? 0 :
			 conn_timeslice_info_.get(compute_index)->get(last_conn_timeslice_[compute_index])->data);
    
    TimesliceInfo* timeslice_info = new TimesliceInfo();
    timeslice_info->data = timeslice_data ;
    timeslice_info->compute_desc = descriptor_index;
    timeslice_info->transmit_time = std::chrono::high_resolution_clock::now();
    assert(conn_timeslice_info_.get(compute_index)->add(timeslice, timeslice_info));
    assert(conn_desc_timeslice_info_.get(compute_index)->add(descriptor_index, timeslice));
    future_conn_timeslices_.get(compute_index)->erase(timeslice);
    ++last_conn_desc_[compute_index];
    last_conn_timeslice_[compute_index] = timeslice;

    check_to_add_rescheduled_timeslices(compute_index);
}

bool InputTimesliceManager::acknowledge_timeslice_rdma_write(uint32_t compute_index, uint64_t timeslice){
    if (!conn_timeslice_info_.get(compute_index)->contains(timeslice)){
    	L_(warning) << "[i_" << scheduler_index_ << "][ACK_RDMA_WRITE] ts " << timeslice << " does not belong to conn_" << compute_index;
    	return false;
    }
    TimesliceInfo* timeslice_info = conn_timeslice_info_.get(compute_index)->get(timeslice);
    if (timeslice_info->rdma_acked_duration != 0)return false;
    timeslice_info->rdma_acked_duration = std::chrono::duration_cast<std::chrono::microseconds>(
	    		std::chrono::high_resolution_clock::now() - timeslice_info->transmit_time).count();
    return true;
}

double InputTimesliceManager::acknowledge_timeslices_completion(uint32_t compute_index, uint64_t up_to_descriptor_id){
    uint64_t sum_latency = 0;
    uint32_t count = 0;
    SizedMap<uint64_t, uint64_t>::iterator transmitted_timeslice_iterator = conn_desc_timeslice_info_.get(compute_index)->get_begin_iterator();

    while (transmitted_timeslice_iterator != conn_desc_timeslice_info_.get(compute_index)->get_end_iterator()
	    && transmitted_timeslice_iterator->first <= up_to_descriptor_id){

	TimesliceInfo* timeslice_info = conn_timeslice_info_.get(compute_index)->get(transmitted_timeslice_iterator->second);
	timeslice_info->completion_acked_duration = std::chrono::duration_cast<std::chrono::microseconds>(
		std::chrono::high_resolution_clock::now() - timeslice_info->transmit_time).count();

	// Calculate the latency
	sum_latency += timeslice_info->completion_acked_duration - timeslice_info->rdma_acked_duration;
	++count;

	assert(conn_desc_timeslice_info_.get(compute_index)->remove(transmitted_timeslice_iterator->first));
	transmitted_timeslice_iterator = conn_desc_timeslice_info_.get(compute_index)->get_begin_iterator();
    }
    return count == 0 ? 0 : sum_latency/count;
}

bool InputTimesliceManager::is_timeslice_rdma_acked(uint32_t compute_index, uint64_t timeslice){
    if (!conn_timeslice_info_.get(compute_index)->contains(timeslice)){
	L_(warning) << "[i_" << scheduler_index_ << "] [RDMA ACKED] ts " << timeslice << " does not belong to conn_" << compute_index;
	return true;
    }

    TimesliceInfo* timeslice_info = conn_timeslice_info_.get(compute_index)->get(timeslice);
    return timeslice_info->rdma_acked_duration != 0 ||  timeslice_info->completion_acked_duration != 0 ? true : false;
}

bool InputTimesliceManager::is_timeslice_belongs_to_timeout_connection(uint64_t timeslice, const std::set<uint32_t> timeout_connections){
    if (timeslice >= next_start_future_timeslice_){
	refill_future_timeslices(timeslice+1);
    }
    std::set<uint32_t>::iterator it = timeout_connections.begin();
    while (it != timeout_connections.end()){
	std::set<uint64_t>* future_ts = future_conn_timeslices_.get(*it);
	if (future_ts->find(timeslice) != future_ts->end() ||
		conn_timeslice_info_.get(*it)->contains(timeslice))
	    return true;
	++it;
    }
    return false;
}

uint32_t InputTimesliceManager::get_compute_connection_count(){
    return compute_count_;
}

uint64_t InputTimesliceManager::get_last_acked_descriptor(uint32_t compute_index){
    if (conn_desc_timeslice_info_.get(compute_index)->empty()){
	if (conn_timeslice_info_.get(compute_index)->empty())return 0;
	return conn_timeslice_info_.get(compute_index)->get(conn_timeslice_info_.get(compute_index)->get_last_key())->compute_desc;
    }
    return conn_desc_timeslice_info_.get(compute_index)->get_begin_iterator()->first - 1;

}

uint64_t InputTimesliceManager::get_timeslice_by_descriptor(uint32_t compute_index, uint64_t descriptor){
    if (conn_desc_timeslice_info_.get(compute_index)->contains(descriptor))
	return conn_desc_timeslice_info_.get(compute_index)->get(descriptor);

    SizedMap<uint64_t, TimesliceInfo*>* timeslice_info_list = conn_timeslice_info_.get(compute_index);
    SizedMap<uint64_t, TimesliceInfo*>::iterator timeslice_info_it = timeslice_info_list->get_end_iterator();
    do{
	--timeslice_info_it;
	if (timeslice_info_it->second->compute_desc == descriptor) return timeslice_info_it->first;
    }while (timeslice_info_it != timeslice_info_list->get_begin_iterator());

    return ConstVariables::MINUS_ONE;
}

uint64_t InputTimesliceManager::get_last_rdma_acked_timeslice(uint32_t compute_index){
    SizedMap<uint64_t, uint64_t>* incompleted_desc_timeslices =conn_desc_timeslice_info_.get(compute_index);
    if (incompleted_desc_timeslices->empty())return last_conn_timeslice_[compute_index];
    SizedMap<uint64_t, uint64_t>::iterator incompleted_it = incompleted_desc_timeslices->get_end_iterator();
    SizedMap<uint64_t, TimesliceInfo*>* conn_timeslice_list = conn_timeslice_info_.get(compute_index);
    TimesliceInfo* timeslice_info;
    do{
	--incompleted_it;
	timeslice_info = conn_timeslice_list->get(incompleted_it->second);
	if (timeslice_info->rdma_acked_duration != 0)return incompleted_it->second;

    }while (incompleted_it != incompleted_desc_timeslices->get_begin_iterator());

    // When all the timeslices in conn_desc_timeslice_info_ are not RDMA acked yet, return the last one not in the conn_desc_timeslice_info_
    return get_timeslice_by_descriptor(compute_index, incompleted_it->first-1);
}


uint64_t InputTimesliceManager::get_last_timeslice_before_blockage(uint32_t timed_out_conn){
    // TODO
    return 0;
}

uint64_t InputTimesliceManager::get_last_connection_descriptor_index(uint32_t compute_index){
    assert (compute_index < last_conn_desc_.size());
    return last_conn_desc_[compute_index];
}

uint64_t InputTimesliceManager::count_timeslices_of_interval(uint32_t compute_index, uint64_t start_ts, uint64_t end_ts){
    uint64_t count = 0;
    SizedMap<uint64_t, TimesliceInfo*>* timesliceInfos = conn_timeslice_info_.get(compute_index);
    if (timesliceInfos->empty())return count;

    SizedMap<uint64_t, TimesliceInfo*>::iterator timesliceInfo_it = timesliceInfos->get_end_iterator();
    do{
	--timesliceInfo_it;
	if (timesliceInfo_it->first >= start_ts && timesliceInfo_it->first <= end_ts)++count;
    }while (timesliceInfo_it != timesliceInfos->get_begin_iterator());
    return count;
}

uint64_t InputTimesliceManager::count_unacked_timeslices_of_interval(uint32_t compute_index, uint64_t start_ts, uint64_t end_ts){
    refill_future_timeslices(end_ts+1);
    uint64_t count = 0;
    SizedMap<uint64_t, uint64_t>* desc_timeslices = conn_desc_timeslice_info_.get(compute_index);
    if (desc_timeslices->empty()) return 0;

    if (desc_timeslices->get_begin_iterator()->second >= start_ts && (--desc_timeslices->get_end_iterator())->second <= end_ts)
	return desc_timeslices->size();

    SizedMap<uint64_t, uint64_t>::iterator desc_timeslice_it = desc_timeslices->get_end_iterator();
    do{
	--desc_timeslice_it;
	if (desc_timeslice_it->second >= start_ts && desc_timeslice_it->second <= end_ts)++count;
    }while (desc_timeslice_it != desc_timeslices->get_begin_iterator());

    return count;
}

uint64_t InputTimesliceManager::count_future_timeslices_of_interval(uint32_t compute_index, uint64_t start_ts, uint64_t end_ts){
    refill_future_timeslices(end_ts+1);
    uint64_t count = 0;
    std::set<uint64_t> future_timeslices = (*future_conn_timeslices_.get(compute_index));
    if (future_timeslices.empty()) return 0;

    if (*future_timeslices.begin() >= start_ts && *(--future_timeslices.end()) <= end_ts)
	return future_timeslices.size();

    for (uint64_t ts:future_timeslices){
	if (ts >= start_ts && ts <= end_ts)++count;
    }
    return count;
}

std::pair<uint64_t, uint64_t> InputTimesliceManager::get_data_and_desc_of_timeslice(uint32_t compute_index, uint32_t timeslice){
    if (!conn_timeslice_info_.get(compute_index)->contains(timeslice)){
	L_(fatal) << "compute index " << compute_index << " timeslice " << timeslice << " last conn_timeslice " << last_conn_timeslice_[compute_index]
		 << " conn_timeslice_info size " << conn_timeslice_info_.get(compute_index)->size();
	assert(false);
    }
    TimesliceInfo* timeslice_info = conn_timeslice_info_.get(compute_index)->get(timeslice);
    return std::pair<uint64_t, uint64_t>(timeslice_info->data, timeslice_info->compute_desc);
}

std::pair<uint64_t, uint64_t> InputTimesliceManager::get_data_and_desc_of_last_timeslice(uint32_t compute_index){
    if (last_conn_timeslice_[compute_index] == ConstVariables::MINUS_ONE)
	return std::pair<uint64_t, uint64_t>(0, 0);
    return get_data_and_desc_of_timeslice(compute_index, last_conn_timeslice_[compute_index]);
}

// TODO REWRITE THIS METHOD!!!
std::vector<uint64_t> InputTimesliceManager::consider_reschedule_decision(HeartbeatFailedNodeInfo failed_node_info,
							 const std::set<uint32_t> timeout_connections){

    assert (compute_count_ > timeout_connections.size());
    std::vector<uint64_t> undo_timeslices;
    if (next_start_future_timeslice_ <= (failed_node_info.timeslice_trigger+1)){
	// Refill the future timeslices until the trigger to have all timeslices that should be distributed over other compute nodes
	refill_future_timeslices(failed_node_info.timeslice_trigger+1);
    }else{
	// Check if timeslice_trigger is already passed!! (return them back to the queue for the correct ordering)
	std::vector<uint64_t> list = undo_transmitted_timeslices_after_trigger(failed_node_info.timeslice_trigger);
	undo_timeslices.insert(undo_timeslices.end(), list.begin(), list.end());
	L_(debug) << "[consider_reschedule_decision][" << failed_node_info.index << "] undo after trigger " << undo_timeslices.size();
	// update the desc correspondingly
	next_start_future_timeslice_ = failed_node_info.timeslice_trigger+1;
    }
    //
    std::set<uint64_t>* failed_timeslice = future_conn_timeslices_.get(failed_node_info.index);

    // clean the conn_desc_timeslice_info_ and move them back to future timeslices
    SizedMap<uint64_t, uint64_t>* failed_conn_desc_timeslice_info = conn_desc_timeslice_info_.get(failed_node_info.index);
    assert (failed_conn_desc_timeslice_info->empty() ||
	    failed_conn_desc_timeslice_info->get_begin_iterator()->first == failed_node_info.last_completed_desc+1);
    while (!failed_conn_desc_timeslice_info->empty()){
	failed_timeslice->insert(failed_conn_desc_timeslice_info->get_begin_iterator()->second);
	undo_timeslices.push_back(failed_conn_desc_timeslice_info->get_begin_iterator()->second);
	failed_conn_desc_timeslice_info->remove(failed_conn_desc_timeslice_info->get_begin_iterator()->first);
    }

	// TODO REMOVE
    if (!failed_timeslice->empty())
	L_(debug) << "Number of failed timeslices is " << failed_timeslice->size() << " trigger timeslice " << failed_node_info.timeslice_trigger << " ... [0] " << *failed_timeslice->begin()
	     << " [n-1] " << *(--failed_timeslice->end());

    // Distribute failed_timeslices over other active connections in a temporary array
    if (!failed_timeslice->empty()){
	std::vector<std::set<uint64_t>*> movable_ts(compute_count_, nullptr);
	uint32_t compute_index = 0;
	while (!failed_timeslice->empty()){
	    while (timeout_connections.find(compute_index) != timeout_connections.end()) compute_index = (compute_index+1)%compute_count_;
	    if (movable_ts[compute_index] == nullptr) movable_ts[compute_index] = new std::set<uint64_t>();
	    movable_ts[compute_index]->insert((*failed_timeslice->begin()));
	    //L_(info) << "ts "<< (*failed_timeslice->begin()) << " of CN " << failed_node_info.index << " moved to " << compute_index;
	    failed_timeslice->erase((*failed_timeslice->begin()));
	    compute_index = (compute_index+1)%compute_count_;
	}
	to_be_moved_timeslices_.add(failed_node_info.timeslice_trigger, movable_ts);
    }

    redistribution_decisions_log_.add(failed_node_info.index, failed_node_info.timeslice_trigger);
    for (uint32_t i=0 ; i < compute_count_ ; ++i){
	L_(debug) << "last transmitted ts of " << i << " is " << last_conn_timeslice_[i] << " and desc " << last_conn_desc_[i];
	check_to_add_rescheduled_timeslices(i);
    }
    return undo_timeslices;
}

std::vector<uint64_t> InputTimesliceManager::update_compute_distribution_frequency(uint64_t start_timeslice, uint64_t last_timeslice, std::vector<uint32_t> compute_frequency){
    // Check whether any timeslice before start_timeslice is sent out
    std::vector<uint64_t> undo_timeslices;
    if (next_start_future_timeslice_ <= start_timeslice){
    	refill_future_timeslices(start_timeslice);
    }else{
	// Check if timeslices after the start_timeslice is already sent out!! (return them back to the queue for the correct ordering)
	std::vector<uint64_t> list = undo_transmitted_timeslices_after_trigger(start_timeslice-1);
	undo_timeslices.insert(undo_timeslices.end(), list.begin(), list.end());
	L_(info) << "[update_compute_distribution_frequency][start_timeslice=" << start_timeslice << "] undo after trigger " << undo_timeslices.size();
	// update the desc correspondingly
	next_start_future_timeslice_ = start_timeslice;
    }
    // TODO Add logging
    virtual_compute_count_ = 0;
    virtual_physical_compute_mapping_.clear();
    bool frequencies_updated = true;
    // Distribute timeslice frequencies in a round-robin scheme
    while(frequencies_updated){
	frequencies_updated = false;
	for (uint32_t i = 0 ; i < compute_frequency.size() ; i++)
	    if (compute_frequency[i] > 0){
		virtual_physical_compute_mapping_.push_back(i);
		--compute_frequency[i];
		++virtual_compute_count_;
		frequencies_updated = true;
	    }
    }

    refill_future_timeslices(last_timeslice+1);
    return undo_timeslices;

}

void InputTimesliceManager::generate_log_files(){
    if (!enable_logging_) return;

/////////////////////////////////////////////////////////////////
    std::ofstream blocked_duration_log_file;
    blocked_duration_log_file.open(log_directory_+"/"+std::to_string(scheduler_index_)+".input.ts_blocked_duration.out");

    blocked_duration_log_file << std::setw(25) << "Timeslice" <<
	std::setw(25) << "Compute Index" <<
	std::setw(25) << "IB" <<
	std::setw(25) << "CB" <<
	std::setw(25) << "MR" <<"\n";

    uint64_t last_ts = std::max((timeslice_IB_blocked_duration_log_.empty() ? 0 : timeslice_IB_blocked_duration_log_.get_last_key()),
			std::max((timeslice_CB_blocked_duration_log_.empty() ? 0 : timeslice_CB_blocked_duration_log_.get_last_key()),
				(timeslice_MR_blocked_duration_log_.empty() ? 0 : timeslice_MR_blocked_duration_log_.get_last_key())));
    for (uint64_t ts = 0 ; ts <= last_ts ; ts++){
	blocked_duration_log_file << std::setw(25) << ts <<
	    std::setw(25) << (ts % compute_count_) <<
	    std::setw(25) << (timeslice_IB_blocked_duration_log_.contains(ts) ? timeslice_IB_blocked_duration_log_.get(ts)/1000.0 : 0) <<
	    std::setw(25) << (timeslice_CB_blocked_duration_log_.contains(ts) ? timeslice_CB_blocked_duration_log_.get(ts)/1000.0 : 0) <<
	    std::setw(25) << (timeslice_MR_blocked_duration_log_.contains(ts) ? timeslice_MR_blocked_duration_log_.get(ts)/1000.0 : 0) << "\n";
    }

    blocked_duration_log_file.flush();
    blocked_duration_log_file.close();


    /////////////////////////////////////////////////////////////////

    std::ofstream block_log_file;
    block_log_file.open(log_directory_+"/"+std::to_string(scheduler_index_)+".input.ts_info.out");

    block_log_file << std::setw(25) << "Timeslice"
	<< std::setw(25) << "Compute Index"
	<< std::setw(25) << "RDMA ACK duration"
	<< std::setw(25) << "Completion ACK duration" << "\n";

    for (uint32_t i = 0 ; i < conn_timeslice_info_.size() ; ++i){
	SizedMap<uint64_t, TimesliceInfo*>::iterator delaying_time = conn_timeslice_info_.get(i)->get_begin_iterator();
	while (delaying_time != conn_timeslice_info_.get(i)->get_end_iterator()){
	block_log_file << std::setw(25) << delaying_time->first
		<< std::setw(25) << i
		<< std::setw(25) << delaying_time->second->rdma_acked_duration
		<< std::setw(25) << delaying_time->second->completion_acked_duration
		<< "\n";
	delaying_time++;
	}
    }
    block_log_file.flush();
    block_log_file.close();
}

uint64_t InputTimesliceManager::log_timeslice_IB_blocked(uint64_t timeslice, bool sent_completed){
    uint64_t duration = ConstVariables::ZERO;
    if (sent_completed){
	if (timeslice_IB_blocked_start_log_.contains(timeslice)){
	    duration = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::high_resolution_clock::now() - timeslice_IB_blocked_start_log_.get(timeslice)).count();
	    timeslice_IB_blocked_duration_log_.add(timeslice, duration);
	    timeslice_IB_blocked_start_log_.remove(timeslice);
	}
    }else{
	timeslice_IB_blocked_start_log_.add(timeslice, std::chrono::high_resolution_clock::now());
    }
    return duration;
}

uint64_t InputTimesliceManager::log_timeslice_CB_blocked(uint64_t timeslice, bool sent_completed){
    uint64_t duration = ConstVariables::ZERO;
    if (sent_completed){
	if (timeslice_CB_blocked_start_log_.contains(timeslice)){
	    duration = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::high_resolution_clock::now() - timeslice_CB_blocked_start_log_.get(timeslice)).count();
	    timeslice_CB_blocked_duration_log_.add(timeslice, duration);
	    timeslice_CB_blocked_start_log_.remove(timeslice);
	}
    }else{
	timeslice_CB_blocked_start_log_.add(timeslice, std::chrono::high_resolution_clock::now());
    }
    return duration;
}

uint64_t InputTimesliceManager::log_timeslice_MR_blocked(uint64_t timeslice, bool sent_completed){
    uint64_t duration = ConstVariables::ZERO;
    if (sent_completed){
	if (timeslice_MR_blocked_start_log_.contains(timeslice)){
	    duration = std::chrono::duration_cast<std::chrono::microseconds>(
		    std::chrono::high_resolution_clock::now() - timeslice_MR_blocked_start_log_.get(timeslice)).count();
	    timeslice_MR_blocked_duration_log_.add(timeslice, duration);
	    timeslice_MR_blocked_start_log_.remove(timeslice);
	}
    }else{
	timeslice_MR_blocked_start_log_.add(timeslice, std::chrono::high_resolution_clock::now());
    }
    return duration;
}


InputTimesliceManager* InputTimesliceManager::instance_ = nullptr;
}