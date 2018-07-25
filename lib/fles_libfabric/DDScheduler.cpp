// Copyright 2018 Farouk Salem <salem@zib.de>


#include "DDScheduler.hpp"

// TO BE REMOVED
#include <fstream>
#include <iomanip>



namespace tl_libfabric
{
// PUBLIC

DDScheduler* DDScheduler::get_instance(){
    if (instance_ == nullptr){
    	instance_ = new DDScheduler();
    }
    return instance_;
}

// TO BE REMOVED
void DDScheduler::init_scheduler(uint32_t input_nodes_count) {
    input_scheduler_count_ = input_nodes_count;
    for (uint_fast16_t i=0 ; i<input_nodes_count ; i++)
	input_scheduler_info_.push_back(new InputSchedulerData());
}

void DDScheduler::init_input_scheduler(uint32_t input_index, std::chrono::system_clock::time_point MPI_time){
    assert(input_scheduler_info_.size() == input_scheduler_count_);
    input_scheduler_info_[input_index]->MPI_Barrier_time = MPI_time;
    input_scheduler_info_[input_index]->clock_offset = std::chrono::duration_cast
    				<std::chrono::microseconds>(begin_time_ - MPI_time).count();
}

void DDScheduler::update_input_connection_count(uint32_t input_conn_count) {
    input_scheduler_count_ = input_conn_count;
}

void DDScheduler::set_scheduler_index(uint32_t index) {
    SCHEDULER_INDEX_ = index;
}


void DDScheduler::set_begin_time(std::chrono::system_clock::time_point begin_time) {
    begin_time_ = begin_time;
}

void DDScheduler::add_actual_meta_data(uint32_t input_index, IntervalMetaData meta_data) {

    if (input_scheduler_info_[input_index]->interval_info_.contains(meta_data.interval_index)) return;

    meta_data.start_time += std::chrono::microseconds(input_scheduler_info_[input_index]->clock_offset);
    input_scheduler_info_[input_index]->interval_info_.add(meta_data.interval_index, meta_data);

    trigger_complete_interval(meta_data.interval_index);
}

const IntervalMetaData* DDScheduler::get_proposed_meta_data(uint32_t input_index, uint64_t interval_index){
    if (actual_interval_meta_data_.empty())return nullptr;

    IntervalMetaData* interval_info;
    if (proposed_interval_meta_data_.contains(interval_index)){
	interval_info = proposed_interval_meta_data_.get(interval_index);
    }else{
	interval_info = calculate_proposed_interval_meta_data(interval_index);
    }
    interval_info->start_time -= std::chrono::microseconds(input_scheduler_info_[input_index]->clock_offset);
    return interval_info;
}

uint64_t DDScheduler::get_last_completed_interval() {
    if (actual_interval_meta_data_.empty())return ConstVariables::MINUS_ONE;
    return actual_interval_meta_data_.get_last_key();
}

    //Generate log files of the stored data
    void DDScheduler::generate_log_files(){
	// TODO organize into small functions
	if (!ConstVariables::ENABLE_LOGGING) return;
	if (true){
	    std::ofstream log_file;
	    log_file.open(std::to_string(SCHEDULER_INDEX_)+".compute.min_max_interval_info.out");

	    log_file << std::setw(25) << "Interval"
		    << std::setw(25) << "Min start"
		    << std::setw(25) << "Max start"
		    << std::setw(25) << "Min duration"
		    << std::setw(25) << "Max duration"
		    << std::setw(25) << "Proposed duration"
		    << std::setw(25) << "Enhanced duration"
		    << std::setw(25) << "Speedup Factor"
		    << std::setw(25) << "Rounds" << "\n";

	    for (SizedMap<uint64_t, IntervalDataLog*>::iterator it = interval_info_logger_.get_begin_iterator();
		    it != interval_info_logger_.get_end_iterator() ; ++it){
		log_file << std::setw(25) << it->first
			<< std::setw(25) << it->second->min_start
			<< std::setw(25) << it->second->max_start
			<< std::setw(25) << it->second->min_duration
			<< std::setw(25) << it->second->max_duration
			<< std::setw(25) << it->second->proposed_duration
			<< std::setw(25) << it->second->enhanced_duration
			<< std::setw(25) << it->second->speedup_applied
			<< std::setw(25) << it->second->rounds_count << "\n";
	    }

	    log_file.flush();
	    log_file.close();
	}
    }

// PRIVATE
// TODO check the constructor
DDScheduler::DDScheduler(){}

void DDScheduler::trigger_complete_interval(const uint64_t interval_index) {
    if (pending_intervals_.contains(interval_index)) pending_intervals_.update(interval_index, pending_intervals_.get(interval_index)+1);
    else pending_intervals_.add(interval_index,1);

    // calculate the unified actual meta-data of the whole interval
    if (pending_intervals_.get(interval_index) == input_scheduler_count_){
	calculate_interval_info(interval_index);
	// TODO uncomment
	//pending_intervals_.remove(interval_index);
    }
}

void DDScheduler::calculate_interval_info(uint64_t interval_index) {
    // TODO optimize this part to be only one loop --> O(n) instead of m*O(n)
    std::chrono::system_clock::time_point average_start_time = get_start_time_statistics(interval_index);// get Average
    uint64_t max_round_duration = get_max_round_duration(interval_index);
    // TODO the round count and the start timeslice should be the same from each input scheduler, otherwise, the interval duration should be increased!
    uint32_t average_round_count = get_average_round_count(interval_index);
    uint64_t average_start_timeslice = get_average_start_timeslice(interval_index);
    uint64_t average_last_timeslice = get_average_last_timeslice(interval_index);

    actual_interval_meta_data_.add(interval_index,
	    new IntervalMetaData(interval_index, average_round_count, average_start_timeslice, average_last_timeslice, average_start_time, max_round_duration*average_round_count));

    // LOGGING
    uint64_t min_start_time = std::chrono::duration_cast<std::chrono::milliseconds>(get_start_time_statistics(interval_index,0,1) - begin_time_).count();
    uint64_t max_start_time = std::chrono::duration_cast<std::chrono::milliseconds>(get_start_time_statistics(interval_index,0,0) - begin_time_).count();
    if (interval_info_logger_.contains(interval_index)) {
	IntervalDataLog* interval_log = interval_info_logger_.get(interval_index);
	interval_log->min_start = min_start_time;
	interval_log->max_start = max_start_time;
	interval_log->min_duration = get_duration_statistics(interval_index,0,1);
	interval_log->max_duration = get_duration_statistics(interval_index,0,0);
    }else{
	interval_info_logger_.add(interval_index, new IntervalDataLog(min_start_time, max_start_time,
		get_duration_statistics(interval_index,0,1), get_duration_statistics(interval_index,0,0), average_round_count));
    }
    //
}

IntervalMetaData* DDScheduler::calculate_proposed_interval_meta_data(uint64_t interval_index) {
    uint64_t last_interval = actual_interval_meta_data_.get_last_key();
    IntervalMetaData* last_interval_info = actual_interval_meta_data_.get(last_interval);
    if (!proposed_interval_meta_data_.empty() && proposed_interval_meta_data_.get_last_key() > last_interval) {
	last_interval = proposed_interval_meta_data_.get_last_key();
	last_interval_info = proposed_interval_meta_data_.get(last_interval);
    }

    uint64_t new_start_timeslice = (last_interval_info->last_timeslice+1) +
	    (interval_index - last_interval_info->interval_index - 1) * (last_interval_info->last_timeslice - last_interval_info->start_timeslice + 1);

    std::chrono::system_clock::time_point new_start_time = last_interval_info->start_time + std::chrono::microseconds(last_interval_info->interval_duration * (interval_index - last_interval));

    uint64_t max_round_duration = get_max_round_duration_history();
    uint64_t new_round_duration = enhance_round_duration(max_round_duration);
    uint32_t round_count = std::ceil(ConstVariables::MIN_INTERVAL_DURATION*1000000.0/(new_round_duration*1.0));
    round_count = round_count == 0 ? 1 : round_count;

    uint64_t new_interval_duration = new_round_duration*round_count;

    uint32_t compute_count = get_last_compute_connection_count();

    IntervalMetaData* new_interval_metadata = new IntervalMetaData(interval_index, round_count, new_start_timeslice, new_start_timeslice + (round_count*compute_count) - 1,
						new_start_time, new_interval_duration);
    proposed_interval_meta_data_.add(interval_index, new_interval_metadata);

    // LOGGING
    interval_info_logger_.add(interval_index, new IntervalDataLog(max_round_duration, new_round_duration, round_count, max_round_duration != new_round_duration ? 1 : 0));

    return new_interval_metadata;
}

uint64_t DDScheduler::enhance_round_duration(uint64_t round_duration) {
    double round_dur_mean_difference = get_mean_round_duration_difference_distory();

    // TODO keep speeding up for an interval of time

    if (round_dur_mean_difference/round_duration*100.0 <= ConstVariables::SPEEDUP_GAP_PERCENTAGE)
	return round_duration - (round_duration*ConstVariables::SPEEDUP_ROUND_FACTOR);
    return round_duration;

}

std::chrono::system_clock::time_point DDScheduler::get_start_time_statistics(uint64_t interval_index, bool average, bool min) {
    std::chrono::system_clock::time_point min_start_time = input_scheduler_info_[0]->interval_info_.get(interval_index).start_time,
	    max_start_time = min_start_time, tmp_time;
    for (uint32_t i = 1; i<input_scheduler_count_ ; i++) {
	tmp_time = input_scheduler_info_[i]->interval_info_.get(interval_index).start_time;
	if (min_start_time > tmp_time) min_start_time = tmp_time;
	if (max_start_time > tmp_time) max_start_time = tmp_time;
    }
    if (average) return min_start_time + std::chrono::microseconds(std::chrono::duration_cast<std::chrono::microseconds>(max_start_time - min_start_time).count()/2);
    return min ? min_start_time : max_start_time;
}

uint64_t DDScheduler::get_duration_statistics(uint64_t interval_index, bool average, bool min) {
    uint64_t min_duration = input_scheduler_info_[0]->interval_info_.get(interval_index).interval_duration,
	    max_duration = min_duration, tmp_duration;
    for (uint32_t i = 1; i<input_scheduler_count_ ; i++) {
	tmp_duration = input_scheduler_info_[i]->interval_info_.get(interval_index).interval_duration;
	if (min_duration > tmp_duration) min_duration = tmp_duration;
	if (max_duration > tmp_duration) max_duration = tmp_duration;
    }
    if (average) return (min_duration + max_duration)/2;
    return min ? min_duration : max_duration;
}

uint64_t DDScheduler::get_max_round_duration(uint64_t interval_index) {
    IntervalMetaData actual_meta_data = input_scheduler_info_[0]->interval_info_.get(interval_index);
    uint64_t round_duration = actual_meta_data.interval_duration/actual_meta_data.round_count, tmp_duration;
    for (uint32_t i = 1; i<input_scheduler_count_ ; i++) {
	actual_meta_data = input_scheduler_info_[i]->interval_info_.get(interval_index);
	tmp_duration = actual_meta_data.interval_duration/actual_meta_data.round_count;
	if (round_duration < tmp_duration)round_duration = tmp_duration;
    }
    return round_duration;
}

uint32_t DDScheduler::get_average_round_count(uint64_t interval_index) {
    uint32_t round_count = 0;
    for (uint32_t i = 0; i<input_scheduler_count_ ; i++)
	round_count += input_scheduler_info_[i]->interval_info_.get(interval_index).round_count;
    return round_count/input_scheduler_count_;
}

uint64_t DDScheduler::get_average_start_timeslice(uint64_t interval_index) {
    uint32_t start_timeslice = 0;
        for (uint32_t i = 0; i<input_scheduler_count_ ; i++)
            start_timeslice += input_scheduler_info_[i]->interval_info_.get(interval_index).start_timeslice;
        return start_timeslice/input_scheduler_count_;
}

uint64_t DDScheduler::get_average_last_timeslice(uint64_t interval_index) {
    uint32_t last_timeslice = 0;
    for (uint32_t i = 0; i<input_scheduler_count_ ; i++)
	last_timeslice += input_scheduler_info_[i]->interval_info_.get(interval_index).last_timeslice;
    return last_timeslice/input_scheduler_count_;
}

uint64_t DDScheduler::get_max_round_duration_history() {
    if (actual_interval_meta_data_.empty()) return 0;

    uint32_t required_size = ConstVariables::SCHEDULER_HISTORY_SIZE, count = 0;
    uint64_t max_round_duration = 0;

    if (actual_interval_meta_data_.size() < required_size) required_size = actual_interval_meta_data_.size();

    for (SizedMap<uint64_t, IntervalMetaData*>::iterator it = --actual_interval_meta_data_.get_end_iterator();
	    it != actual_interval_meta_data_.get_begin_iterator() && count < required_size ; --it, ++count) {
	IntervalMetaData* meta_data = it->second;
	uint64_t average_round_duration = meta_data->interval_duration/meta_data->round_count;
	if (max_round_duration < average_round_duration)
	    max_round_duration = average_round_duration;
    }
    return max_round_duration;
}

double DDScheduler::get_mean_round_duration_difference_distory() {
    uint64_t last_completed_interval = actual_interval_meta_data_.get_last_key();

    // +2 because there is no proposed duration for the first two intervals
    if (last_completed_interval < ConstVariables::SCHEDULER_HISTORY_SIZE+2)return 0;

    double mean = 0;
    uint64_t proposed_round_dur, actual_round_dur;
    IntervalMetaData *proposed_interval, *actual_interval;
    for (uint32_t i=0 ; i< ConstVariables::SCHEDULER_HISTORY_SIZE ; i++){
	proposed_interval = proposed_interval_meta_data_.get(last_completed_interval-i);
	proposed_round_dur = proposed_interval->interval_duration/proposed_interval->round_count;

	actual_interval = actual_interval_meta_data_.get(last_completed_interval-i);
	actual_round_dur = actual_interval->interval_duration/actual_interval->round_count;

	mean += actual_round_dur - proposed_round_dur;
    }
    mean /= ConstVariables::SCHEDULER_HISTORY_SIZE;

    return mean < 0 ? 0 : mean;
}

uint32_t DDScheduler::get_last_compute_connection_count(){
    IntervalMetaData* meta_data = actual_interval_meta_data_.get(actual_interval_meta_data_.get_last_key());
    return (meta_data->last_timeslice - meta_data->start_timeslice+1)/meta_data->round_count;
}

DDScheduler* DDScheduler::instance_ = nullptr;

}
