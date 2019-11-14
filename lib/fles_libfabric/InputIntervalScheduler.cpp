// Copyright 2018 Farouk Salem <salem@zib.de>

#include "InputIntervalScheduler.hpp"

#include <fstream>
#include <iomanip>

namespace tl_libfabric
{
// PUBLIC
InputIntervalScheduler* InputIntervalScheduler::get_instance(uint32_t scheduler_index, uint32_t compute_conn_count,
					    uint32_t interval_length,
					    std::string log_directory, bool enable_logging){
    if (instance_ == nullptr){
	instance_ = new InputIntervalScheduler(scheduler_index, compute_conn_count, interval_length, log_directory, enable_logging);
    }
    return instance_;
}

InputIntervalScheduler* InputIntervalScheduler::get_instance(){
    assert(instance_ != nullptr);
    return instance_;
}

void InputIntervalScheduler::update_compute_connection_count(uint32_t compute_count){
    compute_count_ = compute_count;
}

void InputIntervalScheduler::update_input_scheduler_index(uint32_t scheduler_index){
    scheduler_index_ = scheduler_index;
}

void InputIntervalScheduler::update_input_begin_time(std::chrono::high_resolution_clock::time_point begin_time){
    begin_time_ = begin_time;
    if (interval_info_.empty()){
	// create the first interval
	create_new_interval_info(0);
    }
}

void InputIntervalScheduler::add_proposed_meta_data(const IntervalMetaData meta_data){
    if (!proposed_interval_meta_data_.contains(meta_data.interval_index)){
	proposed_interval_meta_data_.add(meta_data.interval_index,new IntervalMetaData(meta_data));
	if (true){
	    L_(info) << "[i " << scheduler_index_ << "] "
		      << "interval"
		      << meta_data.interval_index
		      << "[TSs "
		      << meta_data.start_timeslice
		      << " to "
		      << meta_data.last_timeslice
		      << " is proposed and should start after "
		      << std::chrono::duration_cast<std::chrono::microseconds>(meta_data.start_time - std::chrono::high_resolution_clock::now()).count()
		      << " us & take " << meta_data.interval_duration << " us";
	}
    }
}

const IntervalMetaData* InputIntervalScheduler::get_actual_meta_data(uint64_t interval_index){
    return actual_interval_meta_data_.contains(interval_index) ? actual_interval_meta_data_.get(interval_index) : nullptr;
}

uint64_t InputIntervalScheduler::get_last_timeslice_to_send(){
    InputIntervalInfo* current_interval = interval_info_.get(interval_info_.get_last_key());
    uint64_t next_round = get_interval_expected_round_index(current_interval->index)+1; // get_interval_current_round_index(current_interval->index)+1
    return std::min(current_interval->start_ts + (next_round*current_interval->num_ts_per_round) - 1, current_interval->end_ts);
}

void InputIntervalScheduler::increament_sent_timeslices(){
    InputIntervalInfo* current_interval = interval_info_.get(interval_info_.get_last_key());
    if (current_interval->count_sent_ts == 0)
	current_interval->actual_start_time = std::chrono::high_resolution_clock::now();
    current_interval->count_sent_ts++;
    if (is_interval_sent_completed(current_interval->index) && is_ack_percentage_reached(current_interval->index) && !interval_info_.contains(current_interval->index+1)){
    	create_new_interval_info(current_interval->index+1);
    }
}

void InputIntervalScheduler::increament_acked_timeslices(uint64_t timeslice){
    InputIntervalInfo* current_interval = get_interval_of_timeslice(timeslice);
    if (current_interval == nullptr)return;
    current_interval->count_acked_ts++;
    if (is_ack_percentage_reached(current_interval->index) && is_interval_sent_completed(current_interval->index) && !interval_info_.contains(current_interval->index+1)){
	create_new_interval_info(current_interval->index+1);
    }
    if (is_interval_sent_ack_completed(current_interval->index)){
	create_actual_interval_meta_data(current_interval);
    }
}

int64_t InputIntervalScheduler::get_next_fire_time(){
    uint64_t interval = interval_info_.get_last_key();
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    current_interval->rounds_counter++;

    std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now();

    // If  no proposed duration or the proposed finish time is reached, then send as fast as possible.
    if (current_interval->duration_per_ts == 0 ||
	(!is_ack_percentage_reached(interval) &&
	    (current_interval->proposed_start_time + std::chrono::microseconds(current_interval->proposed_duration)) < now))
	return ConstVariables::ZERO;

    // If the interval is new
    if (current_interval->count_sent_ts == 0) {
	if (current_interval->proposed_start_time <= now) return ConstVariables::ZERO;
	return std::chrono::duration_cast<std::chrono::microseconds>(current_interval->proposed_start_time - now).count();
    }

    uint64_t expected_sent_ts = get_expected_sent_ts_count(interval);

    int64_t duration = current_interval->duration_per_round + ((current_interval->count_sent_ts - expected_sent_ts) * current_interval->duration_per_ts);
    if (duration < 0) duration = 0;

    return duration;
/*
    uint32_t expected_round = current_interval->count_sent_ts/current_interval->num_ts_per_round;

    std::chrono::high_resolution_clock::time_point expected_round_time = get_expected_round_sent_time(interval, expected_round);//get_interval_time_to_expected_round(interval);
    return expected_round_time < std::chrono::high_resolution_clock::now() ? std::chrono::high_resolution_clock::now() : expected_round_time;
*/
}

uint32_t InputIntervalScheduler::get_compute_connection_count(){
    return compute_count_;
}

// PRIVATE
InputIntervalScheduler::InputIntervalScheduler(uint32_t scheduler_index, uint32_t compute_conn_count,
	uint32_t interval_length, std::string log_directory, bool enable_logging):
    		scheduler_index_(scheduler_index), compute_count_(compute_conn_count),
		interval_length_(interval_length), log_directory_(log_directory),
		enable_logging_(enable_logging) {
}

void InputIntervalScheduler::create_new_interval_info(uint64_t interval_index){
    InputIntervalInfo* new_interval_info = nullptr;

    if (proposed_interval_meta_data_.contains(interval_index)){// proposed info exist
	IntervalMetaData* proposed_meta_data = proposed_interval_meta_data_.get(interval_index);

	// check if there is a gap in ts due to un-received meta-data
	const InputIntervalInfo* prev_interval = interval_info_.get(interval_index-1);
	new_interval_info = new InputIntervalInfo(interval_index, proposed_meta_data->round_count, prev_interval->end_ts+1, proposed_meta_data->last_timeslice, proposed_meta_data->start_time, proposed_meta_data->interval_duration);
    }else{
	if (interval_info_.empty()){// first interval
	    // TODO check the initial INTERVAL_LENGTH_ & COMPUTE_COUNT_;
	    uint32_t round_count = floor(interval_length_/compute_count_);
	    new_interval_info = new InputIntervalInfo(interval_index, round_count, 0, (round_count*compute_count_)-1, std::chrono::high_resolution_clock::now(), 0);

	}else{// following last proposed meta-data
	    // TODO wait for the proposing!!!
	    InputIntervalInfo* prev_interval = interval_info_.get(interval_index-1);
	    new_interval_info = new InputIntervalInfo(interval_index, prev_interval->round_count, prev_interval->end_ts+1,
		    prev_interval->end_ts + (prev_interval->round_count*compute_count_),
		    prev_interval->proposed_start_time + std::chrono::microseconds(prev_interval->proposed_duration), prev_interval->proposed_duration);
	}
    }

    if (true){
	L_(info) << "[i " << scheduler_index_ << "] "
		      << "interval"
		      << interval_index
		      << "[TSs "
		      << new_interval_info->start_ts
		      << " to "
		      << new_interval_info->end_ts
		      << "] should start after "
		      << std::chrono::duration_cast<std::chrono::microseconds>(new_interval_info->proposed_start_time - std::chrono::high_resolution_clock::now()).count()
		      << " us & take " << new_interval_info->proposed_duration << " us";
    }

    interval_info_.add(interval_index, new_interval_info);
}

void InputIntervalScheduler::create_actual_interval_meta_data(InputIntervalInfo* interval_info){
    interval_info->actual_duration = std::chrono::duration_cast<std::chrono::microseconds>(
		std::chrono::high_resolution_clock::now() - interval_info->actual_start_time).count();
    IntervalMetaData* actual_metadata = new IntervalMetaData(interval_info->index, interval_info->round_count, interval_info->start_ts, interval_info->end_ts,
	    interval_info->actual_start_time,interval_info->actual_duration);
    if (true){
	L_(info) << "[i " << scheduler_index_ << "] "
		<< "interval"
                << actual_metadata->interval_index
                << "[TSs "
                << actual_metadata->start_timeslice
                << " to "
                << actual_metadata->last_timeslice
                << "] is finished and delayed for "
                << std::chrono::duration_cast<std::chrono::microseconds>(actual_metadata->start_time - interval_info->proposed_start_time).count()
                << " us & took " << actual_metadata->interval_duration << " us in " << interval_info->rounds_counter << " rounds";
    }
    actual_interval_meta_data_.add(interval_info->index, actual_metadata);
}

uint64_t InputIntervalScheduler::get_expected_sent_ts_count(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    if (current_interval->duration_per_ts == 0)return (current_interval->end_ts-current_interval->start_ts+1);
    std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now();
    if (now < current_interval->actual_start_time) return 0;
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - current_interval->actual_start_time).count() / current_interval->duration_per_ts;
}

// TODO Unused
uint64_t InputIntervalScheduler::get_expected_last_sent_ts(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    if (current_interval->duration_per_ts == 0)return current_interval->end_ts;
    std::chrono::high_resolution_clock::time_point now = std::chrono::high_resolution_clock::now();
    if (now < current_interval->actual_start_time) return 0;
    return current_interval->start_ts + get_expected_sent_ts_count(interval) - 1;
}

// TODO Unused partially
uint64_t InputIntervalScheduler::get_interval_current_round_index(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    return current_interval->count_sent_ts / current_interval->num_ts_per_round;
}

uint64_t InputIntervalScheduler::get_interval_expected_round_index(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    return get_expected_sent_ts_count(interval) / current_interval->num_ts_per_round;
}

InputIntervalInfo* InputIntervalScheduler::get_interval_of_timeslice(uint64_t timeslice){
    SizedMap<uint64_t, InputIntervalInfo*>::iterator end_it = interval_info_.get_end_iterator();
    do{
	--end_it;
	if (timeslice >= end_it->second->start_ts && timeslice <= end_it->second->end_ts)return end_it->second;
    }while (end_it != interval_info_.get_begin_iterator());
    return nullptr;
}

bool InputIntervalScheduler::is_interval_sent_completed(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    return current_interval->count_sent_ts == (current_interval->end_ts-current_interval->start_ts+1) ? true: false;
}

bool InputIntervalScheduler::is_interval_sent_ack_completed(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    return is_interval_sent_completed(interval) && current_interval->count_sent_ts == current_interval->count_acked_ts ? true: false;
}

bool InputIntervalScheduler::is_ack_percentage_reached(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    // TODO change the percentage to be configurable
    return (current_interval->count_acked_ts*1.0)/((current_interval->end_ts-current_interval->start_ts+1)*1.0) >= 0.95 ? true: false;
}

std::chrono::high_resolution_clock::time_point InputIntervalScheduler::get_expected_ts_sent_time(uint64_t interval, uint64_t timeslice){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    return current_interval->actual_start_time + std::chrono::microseconds((timeslice - current_interval->start_ts) * current_interval->duration_per_ts);
}

std::chrono::high_resolution_clock::time_point InputIntervalScheduler::get_expected_round_sent_time(uint64_t interval, uint64_t round) {
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    return current_interval->actual_start_time + std::chrono::microseconds(round * current_interval->duration_per_round);
}

std::chrono::high_resolution_clock::time_point InputIntervalScheduler::get_interval_time_to_expected_round(uint64_t interval){
    InputIntervalInfo* current_interval = interval_info_.get(interval);
    // TODO this line would congest the network/network card because the scheduler could be late and want to catch up.
    return current_interval->actual_start_time + std::chrono::microseconds(current_interval->duration_per_round*get_interval_expected_round_index(interval));
    //return std::chrono::high_resolution_clock::now() + std::chrono::microseconds(current_interval->duration_per_round);
}

// TO BO REMOVED
void InputIntervalScheduler::log_timeslice_transmit_time(uint64_t timeslice, uint32_t compute_index){
    InputIntervalInfo* interval_info = get_interval_of_timeslice(timeslice);
    if (interval_info == nullptr)return;
    TimesliceInfo* timeslice_info = new TimesliceInfo();
    timeslice_info->expected_time = get_expected_ts_sent_time(interval_info->index, timeslice);
    timeslice_info->compute_index = compute_index;
    timeslice_info->transmit_time = std::chrono::high_resolution_clock::now();
    timeslice_info_log_.add(timeslice, timeslice_info);

    uint64_t round_index = floor((timeslice-interval_info->start_ts+1)/interval_info->num_ts_per_round*1.0);
    std::pair<uint64_t,uint64_t> interval_round_pair = std::make_pair(interval_info->index, round_index);
    if (!round_proposed_actual_start_time_log_.contains(interval_round_pair)){
	std::pair<uint64_t,uint64_t> expected_actual_pair = std::make_pair(
		std::chrono::duration_cast<std::chrono::microseconds>(get_expected_round_sent_time(interval_info->index, round_index) - begin_time_).count(),
		std::chrono::duration_cast<std::chrono::microseconds>(timeslice_info->transmit_time - begin_time_).count());
	round_proposed_actual_start_time_log_.add(interval_round_pair,expected_actual_pair);
    }
}

void InputIntervalScheduler::generate_log_files(){
    if (!enable_logging_) return;

    std::ofstream log_file;
    log_file.open(log_directory_+"/"+std::to_string(scheduler_index_)+".input.proposed_actual_interval_info.out");

    log_file << std::setw(25) << "Interval" <<
	    std::setw(25) << "proposed time" <<
	    std::setw(25) << "Actual time" <<
	    std::setw(25) << "Proposed duration" <<
	    std::setw(25) << "Actual duration" << "\n";

    SizedMap<uint64_t, IntervalMetaData*>::iterator it_actual = actual_interval_meta_data_.get_begin_iterator();
    IntervalMetaData* proposed_metadata = nullptr;
    uint64_t proposed_time, actual_time;
    while (it_actual != actual_interval_meta_data_.get_end_iterator()){
	proposed_metadata = proposed_interval_meta_data_.contains(it_actual->first) ? proposed_interval_meta_data_.get(it_actual->first): nullptr;
	proposed_time = proposed_metadata == nullptr ? 0 : std::chrono::duration_cast<std::chrono::milliseconds>(proposed_metadata->start_time - begin_time_).count();
	actual_time = std::chrono::duration_cast<std::chrono::milliseconds>(it_actual->second->start_time - begin_time_).count();
	log_file << std::setw(25) << it_actual->first
		<< std::setw(25) << proposed_time
		<< std::setw(25) << actual_time
		<< std::setw(25) << (proposed_metadata == nullptr ? 0 : proposed_metadata->interval_duration)
		<< std::setw(25) << it_actual->second->interval_duration << "\n";

	it_actual++;
    }
    log_file.flush();
    log_file.close();

    /////////////////////////////////////////////////////////////////

    std::ofstream block_log_file;
    block_log_file.open(log_directory_+"/"+std::to_string(scheduler_index_)+".input.ts_info.out");

    block_log_file << std::setw(25) << "Timeslice"
	<< std::setw(25) << "Compute Index"
	<< std::setw(25) << "duration"
	<< std::setw(25) << "delay" << "\n";

    SizedMap<uint64_t, TimesliceInfo*>::iterator delaying_time = timeslice_info_log_.get_begin_iterator();
    while (delaying_time != timeslice_info_log_.get_end_iterator()){
    block_log_file << std::setw(25) << delaying_time->first
	    << std::setw(25) << delaying_time->second->compute_index
	    << std::setw(25) << delaying_time->second->acked_duration
	    << std::setw(25) << std::chrono::duration_cast<std::chrono::microseconds>(
		delaying_time->second->transmit_time - delaying_time->second->expected_time).count()
	    << "\n";
    delaying_time++;
    }
    block_log_file.flush();
    block_log_file.close();

    /////////////////////////////////////////////////////////////////

    std::ofstream expected_actual_round_time_log_file;
    expected_actual_round_time_log_file.open(log_directory_+"/"+std::to_string(scheduler_index_)+".input.expected_actual_round_start_time.out");

    expected_actual_round_time_log_file << std::setw(25) << "Round" <<
    std::setw(25) << "Expected Time" <<
    std::setw(25) << "Actual Time" <<
    std::setw(25) << "Diff" << "\n";

    SizedMap<std::pair<uint64_t, uint64_t>, std::pair<uint64_t, uint64_t>>::iterator expected_actual_round_time_log = round_proposed_actual_start_time_log_.get_begin_iterator();
    while (expected_actual_round_time_log != round_proposed_actual_start_time_log_.get_end_iterator()){
	uint64_t round_index = expected_actual_round_time_log->first.first* interval_info_.get(interval_info_.get_last_key())->round_count + expected_actual_round_time_log->first.second;
	int64_t expected_time = expected_actual_round_time_log->second.first/1000.0,
		actual_time = expected_actual_round_time_log->second.second/1000.0;
	expected_actual_round_time_log_file << std::setw(25) << round_index <<
		std::setw(25) << expected_time <<
		std::setw(25) << actual_time <<
		std::setw(25) << (actual_time-expected_time) << "\n";
	expected_actual_round_time_log++;
    }
    expected_actual_round_time_log_file.flush();
    expected_actual_round_time_log_file.close();
}

InputIntervalScheduler* InputIntervalScheduler::instance_ = nullptr;

}
