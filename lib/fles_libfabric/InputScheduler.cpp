// Copyright 2018 Farouk Salem <salem@zib.de>

#include "InputScheduler.hpp"

namespace tl_libfabric
{



// PUBLIC
InputScheduler* InputScheduler::get_instance(){
    if (instance_ == nullptr){
	instance_ = new InputScheduler();
    }
    return instance_;
}

void InputScheduler::add_proposed_meta_data(IntervalMetaData* meta_data){
    if (!proposed_interval_meta_data_.contains(meta_data->interval_index)){
	proposed_interval_meta_data_.add(meta_data->interval_index,meta_data);
    }
}

IntervalMetaData* InputScheduler::get_actual_meta_data(uint64_t interval_index){
    return actual_interval_meta_data_.contains(interval_index) ? actual_interval_meta_data_.get(interval_index) : nullptr;
}

uint64_t InputScheduler::get_last_timeslice_to_send(){
    InputIntervalInfo* current_interval = interval_info_.get(interval_info_.get_last_key());
    uint64_t next_round = get_interval_current_round_index(current_interval->index)+1;
    return current_interval->start_ts + (next_round*current_interval->num_ts_per_round) - 1;
}

void InputScheduler::increament_sent_timeslices(){
    interval_info_.get(interval_info_.get_last_key())->count_sent_ts++;
    // TODO create new interval
}

void InputScheduler::increament_acked_timeslices(uint64_t timeslice){
    InputIntervalInfo* current_interval = get_interval_of_timeslice(timeslice);
    current_interval->count_acked_ts++;
    if (is_interval_sent_ack_completed(current_interval->index)){
	create_actual_interval_meta_data(current_interval);
	create_new_interval_info(current_interval->index+1);
    }
}

std::chrono::system_clock::time_point InputScheduler::get_next_fire_time(){
    uint64_t interval = interval_info_.get_last_key();
    InputIntervalInfo* current_interval = interval_info_.get(interval);

    // If  no proposed duration or the proposed finish time is reached, then send as fast as possible.
    if (current_interval->duration_per_ts == 0 ||
	(!is_ack_percentage_reached(interval) &&
	    (current_interval->proposed_start_time + std::chrono::microseconds(current_interval->proposed_duration)) < std::chrono::system_clock::now()))
	return std::chrono::system_clock::now();

    std::chrono::system_clock::time_point expected_round_time = get_interval_time_to_expected_round(interval);
    return expected_round_time < std::chrono::system_clock::now() ? std::chrono::system_clock::now() : expected_round_time;

}


// PRIVATE

InputScheduler::InputScheduler(){
    // create the first interval
    create_new_interval_info(0);
}

void InputScheduler::create_new_interval_info(uint64_t interval_index){
    InputIntervalInfo* new_interval_info = nullptr;

    if (proposed_interval_meta_data_.contains(interval_index)){// proposed info exist
	IntervalMetaData* proposed_meta_data = proposed_interval_meta_data_.get(interval_index);

	// check if there is a gap in ts due to un-received meta-data
	InputIntervalInfo* prev_interval = interval_info_.get(interval_index-1);
	uint64_t start_timeslice = prev_interval->end_ts+1 != start_timeslice ? proposed_meta_data->start_timeslice: prev_interval->end_ts+1;

	new_interval_info = new InputIntervalInfo(interval_index, proposed_meta_data->round_count, start_timeslice, proposed_meta_data->last_timeslice, proposed_meta_data->start_time, proposed_meta_data->interval_duration);
    }else{
	if (interval_info_.empty()){// first interval
	    // TODO check the initial INTERVAL_LENGTH_ & COMPUTE_COUNT_;
	    new_interval_info = new InputIntervalInfo(interval_index, INTERVAL_LENGTH_, 0, COMPUTE_COUNT_, std::chrono::system_clock::now(), 0);

	}else{// following last proposed meta-data
	    InputIntervalInfo* prev_interval = interval_info_.get(interval_index-1);
	    new_interval_info = new InputIntervalInfo(interval_index, prev_interval->round_count, prev_interval->end_ts+1,
		    prev_interval->end_ts + (prev_interval->round_count*COMPUTE_COUNT_),
		    prev_interval->proposed_start_time + std::chrono::microseconds(prev_interval->proposed_duration), prev_interval->proposed_duration);
	}
    }

    interval_info_.add(interval_index, new_interval_info);
}

void InputScheduler::create_actual_interval_meta_data(InputIntervalInfo* interval_info){
    IntervalMetaData* actual_metadata = new IntervalMetaData(interval_info->index, interval_info->round_count, interval_info->start_ts, interval_info->end_ts,
	    interval_info->actual_start_time,interval_info->actual_duration);
    actual_interval_meta_data_.add(interval_info->index, actual_metadata);
}

uint64_t InputScheduler::get_expected_sent_ts(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    if (current_interval->duration_per_ts == 0)return (current_interval->end_ts-current_interval->start_ts+1);
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - current_interval->actual_start_time).count() / current_interval->duration_per_ts;
}

uint64_t InputScheduler::get_interval_current_round_index(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    return (uint64_t)(current_interval->count_sent_ts / current_interval->num_ts_per_round);
}

uint64_t InputScheduler::get_interval_expected_round_index(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    return (uint64_t)(get_expected_sent_ts(interval) / current_interval->num_ts_per_round);
}

InputIntervalInfo* InputScheduler::get_interval_of_timeslice(uint64_t timeslice){
    SizedMap<uint64_t, InputIntervalInfo*>::iterator end_it = interval_info_.get_end_iterator();
    do{
	--end_it;
	if (timeslice >= end_it->second->start_ts && timeslice <= end_it->second->end_ts)return end_it->second;
    }while (end_it != interval_info_.get_begin_iterator());
    return nullptr;
}

bool InputScheduler::is_interval_sent_completed(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    return current_interval->count_sent_ts == (current_interval->end_ts-current_interval->start_ts+1) ? true: false;
}


bool InputScheduler::is_ack_percentage_reached(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    // TODO change the percentage to be configurable
    return (current_interval->count_acked_ts*1.0)/((current_interval->end_ts-current_interval->start_ts+1)*1.0) >= 0.7 ? true: false;
}

bool InputScheduler::is_interval_sent_ack_completed(uint64_t interval){
    return is_interval_sent_completed(interval) && is_ack_percentage_reached(interval) ? true: false;
}

std::chrono::system_clock::time_point InputScheduler::get_expected_ts_sent_time(uint64_t interval, uint64_t timeslice){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    return current_interval->actual_start_time + std::chrono::microseconds((timeslice - current_interval->start_ts) * current_interval->duration_per_ts);
}

std::chrono::system_clock::time_point InputScheduler::get_interval_time_to_expected_round(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    return current_interval->actual_start_time + std::chrono::microseconds(current_interval->duration_per_round*get_interval_expected_round_index(interval));
}

InputScheduler* InputScheduler::instance_ = nullptr;
// TODO
uint64_t InputScheduler::INTERVAL_LENGTH_=1000, COMPUTE_COUNT_=8;

}
