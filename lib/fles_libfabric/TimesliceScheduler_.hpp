/*
 * TimesliceScheduler.hpp
 *
 *  Created on: Aug 4, 2017
 *      Author: Farouk Salem
 */

#pragma once

#include "InputSchedulerData.hpp"
#include <algorithm>
#include <set>
#include <vector>
#include <chrono>
#include <assert.h>
#include <log.hpp>
#include <fstream>
#include <iomanip>
#include <math.h>

namespace tl_libfabric {


class TimesliceScheduler {
public:
	TimesliceScheduler(const uint64_t compute_index,
		const uint32_t input_node_count, const uint32_t interval_length):
			compute_index_(compute_index), input_node_count_(input_node_count), INTERVAL_LENGTH(interval_length),
			proposed_interval_start_time_info_(MAX_DURATION_HISTORY), actual_interval_start_time_info_(MAX_DURATION_HISTORY),
			acked_interval_count_(MAX_DURATION_HISTORY), sum_median_interval_duration_(MAX_DURATION_HISTORY){

		for (uint_fast16_t i=0 ; i<input_node_count_ ; i++)
			sender_info_.push_back(InputSchedulerData());
	}

	TimesliceScheduler() = delete;
	TimesliceScheduler(const TimesliceScheduler&) = delete;
	TimesliceScheduler& operator=(const TimesliceScheduler&) = delete;

	/// set the MPI barrier time of TimesliceBuilder
	void set_compute_MPI_time(
			std::chrono::high_resolution_clock::time_point compute_MPI_time){
		compute_MPI_time_ = compute_MPI_time;
	}

	/// Init compute info
	void init_compute_time(uint64_t compute_index, uint32_t input_node_count,
			std::chrono::high_resolution_clock::time_point compute_MPI_time){
		compute_index_ = compute_index;
		input_node_count_ = input_node_count;
		compute_MPI_time_ = compute_MPI_time;
	}

	/// This method initializes the required data from each input node such as when the MPI barrier is passed
	void init_input_index_info(uint32_t input_index,
			std::chrono::high_resolution_clock::time_point MPI_time){

		assert(sender_info_.size() == input_node_count_);
		sender_info_[input_index].MPI_Barrier_time = MPI_time;
		sender_info_[input_index].clock_offset = std::chrono::duration_cast
				<std::chrono::microseconds>(compute_MPI_time_ - MPI_time).count();
	}

	/// This method adds the received information from an input node to the scheduler data
	void add_input_interval_info(uint32_t input_index, IntervalMetaData interval_metadata){

	    if (!sender_info_[input_index].interval_info_.contains(interval_metadata.interval_index)) {

		uint64_t round_duration = interval_metadata.interval_duration/interval_metadata.round_count;

		sender_info_[input_index].interval_info_.add(interval_metadata.interval_index, interval_metadata);
		sender_info_[input_index].round_durations.add(round_duration);

		if (!sum_median_interval_duration_.contains(interval_metadata.interval_index)){
		    sum_median_interval_duration_.add(interval_metadata.interval_index, ConstVariables::ZERO);
		}
		SizedMap<uint64_t, uint64_t>::iterator sum_it = sum_median_interval_duration_.get_iterator(interval_metadata.interval_index);
		sum_it->second += sender_info_[input_index].round_durations.get_median_key();
		increament_acked_interval(interval_metadata.interval_index);
	    }

	}

	/// This method retrieves the interval information that will be sent to input nodes
	const IntervalMetaData get_interval_info(uint64_t interval_index,uint32_t input_index){

	    if (!proposed_interval_start_time_info_.contains(interval_index)){
		proposed_interval_start_time_info_.add(interval_index, get_interval_sent_time(interval_index));
	    }

	    IntervalMetaData interval_info = proposed_interval_start_time_info_.get(interval_index);
	    interval_info.start_time -= std::chrono::microseconds(sender_info_[input_index].clock_offset);
	    return interval_info;
	}

	uint64_t get_last_completed_interval(){

	    return last_completed_interval_;
	}

	void build_scheduled_time_file(){

	    /*std::ofstream log_file;
	    log_file.open(std::to_string(compute_index_)+".compute.proposed_vs_sent_time.out");

	    log_file << std::setw(25) << "Input Index" << std::setw(25) << "Timeslice" << std::setw(25) << "Contribution" << std::setw(25) << "Proposed(t)" << std::setw(25) << "Sent(t)" << std::setw(25) << "Diff" << std::setw(25) << "Duration" << "\n";

	    std::map<uint64_t, std::vector<std::pair<int64_t, int64_t> > >::iterator it = proposed_actual_times_log_.begin();
	    std::vector<std::pair<int64_t, int64_t>> times;

	    while (it != proposed_actual_times_log_.end()){
		times = it->second;

		for (uint32_t i=0 ; i<times.size() ; i++){
		    log_file << std::setw(25) << i << std::setw(25) << it->first << std::setw(25) << (it->first+i) << std::setw(25) << (times[i].first*1.0)/1000.0 << std::setw(25) <<
			    (times[i].second*1.0)/1000.0 << std::setw(25) << ((times[i].second - times[i].first)*1.0)/1000.0 << std::setw(25) <<
			    (durations_log_.find(it->first)->second*1.0)/1000.0 << "\n";
		}

		log_file.flush();
		++it;
	    }


	    log_file.close();*/
	}

	void build_duration_file(){

	    if (!ConstVariables::ENABLE_LOGGING) return;
	    if (true){
		std::ofstream log_file;
		log_file.open(std::to_string(compute_index_)+".compute.min_max_interval_info.out");

		log_file << std::setw(25) << "Interval"
			<< std::setw(25) << "Min start"
			<< std::setw(25) << "Max start"
			<< std::setw(25) << "Min duration"
			<< std::setw(25) << "Max duration"
			<< std::setw(25) << "Proposed duration"
			<< std::setw(25) << "Speedup Factor"
			<< std::setw(25) << "Rounds" << "\n";

		std::map<uint64_t, std::pair<int64_t, int64_t> >::iterator times_it = min_max_interval_start_time_log_.begin(),
			dur_it = min_max_interval_duration_log_.begin();
		std::map<uint64_t, double>::iterator speedup_factor;
		std::map<uint64_t, uint32_t>::iterator interval_round;
		std::map<uint64_t, std::pair<uint64_t, uint64_t> >::iterator duration_it;

		while (times_it != min_max_interval_start_time_log_.end() && dur_it != min_max_interval_duration_log_.end()){
		    double factor = 0.0;
		    uint32_t round_count = ConstVariables::MAX_TIMESLICE_PER_INTERVAL;
		    uint64_t proposed_duration = 0;

		    speedup_factor = speedup_duration_factor_log_.find(times_it->first);
		    interval_round = interval_round_log_.find(times_it->first);
		    if (speedup_factor != speedup_duration_factor_log_.end()){
			factor = speedup_factor->second;
			round_count = interval_round->second;
		    }

		    duration_it = proposed_median_enhanced_duration_log_.find(times_it->first);
		    if (duration_it != proposed_median_enhanced_duration_log_.end())
			proposed_duration = duration_it->second.first/1000.0;

		    log_file << std::setw(25) << times_it->first
			    << std::setw(25) << times_it->second.first
			    << std::setw(25) << times_it->second.second
			    << std::setw(25) << dur_it->second.first
			    << std::setw(25) << dur_it->second.second
			    << std::setw(25) << proposed_duration
			    << std::setw(25) << factor
			    << std::setw(25) << round_count << "\n";
		    ++times_it;
		    ++dur_it;
		}


		log_file.flush();
		log_file.close();
	    }
	    if (true){
		std::ofstream log_file;
		log_file.open(std::to_string(compute_index_)+".compute.proposed_median_enhanced_duration.out");

		log_file << std::setw(25) << "Interval"
			<< std::setw(25) << "Median"
			<< std::setw(25) << "Enhanced" << "\n";

		std::map<uint64_t, std::pair<uint64_t, uint64_t> >::iterator duration_it = proposed_median_enhanced_duration_log_.begin();

		while (duration_it != proposed_median_enhanced_duration_log_.end()){

		    log_file << std::setw(25) << duration_it->first
			    << std::setw(25) << duration_it->second.first
			    << std::setw(25) << duration_it->second.second << "\n";
		    ++duration_it;
		}
		log_file.flush();
		log_file.close();
	    }
	}

private:
	/// This increases the counter for the received intervals to trigger when to start calculate the sent time
	void increament_acked_interval(uint64_t interval_index) {

	    uint32_t count = 1;
	    if (acked_interval_count_.contains(interval_index)) {
		count = acked_interval_count_.get(interval_index)+1;
		acked_interval_count_.update(interval_index,count);
	    } else {
		acked_interval_count_.add(interval_index, count);
	    }
	    if (count == input_node_count_) {
		calculate_interval_info(interval_index);
	    }
	}

	/// This calculates the needed duration to complete an interval from all input nodes
	void calculate_interval_info(uint64_t interval_index){

	    std::chrono::high_resolution_clock::time_point min_start_time, max_start_time, median_start_time, tmp;
	    std::vector<uint64_t> round_durations;
	    /// If an input scheduler doesn't receive the updated info from the compute scheduler, the last received data would be used
	    std::vector<uint32_t> round_counts;
	    std::vector<uint32_t> start_timeslices;

	    // get the earliest start time!
	    for (uint32_t indx = 0; indx < input_node_count_ ; indx++){
		if (round_durations.empty()){
		    min_start_time = sender_info_[indx].interval_info_.get(interval_index).start_time;
		    max_start_time = min_start_time;
		}
		round_durations.push_back(sender_info_[indx].interval_info_.get(interval_index).interval_duration/sender_info_[indx].interval_info_.get(interval_index).round_count);
		round_counts.push_back(sender_info_[indx].interval_info_.get(interval_index).round_count);
		start_timeslices.push_back(sender_info_[indx].interval_info_.get(interval_index).start_timeslice);
		tmp = sender_info_[indx].interval_info_.get(interval_index).start_time;
		if (tmp < min_start_time){
		    min_start_time = tmp;
		}

		if (tmp > max_start_time){
		    max_start_time = tmp;
		}
	    }
	    std::sort(round_durations.begin(), round_durations.end());
	    std::sort(round_counts.begin(), round_counts.end());
	    std::sort(start_timeslices.begin(), start_timeslices.end());

	    uint64_t median_round_duration;
	    if (round_durations.size() >= 2 && round_durations.size() % 2 == 0){
		int mid_index = round_durations.size()/2;
		median_round_duration = (round_durations[mid_index-1] + round_durations[mid_index])/2;
	    }else{
		median_round_duration = round_durations[round_durations.size()/2];
	    }

	    median_start_time = min_start_time + std::chrono::microseconds(std::chrono::duration_cast<std::chrono::microseconds>(max_start_time - min_start_time).count()/2);
	    if (false){
		L_(info) << "[" << compute_index_ << "] interval "
			<< interval_index << " took "
			<< (median_round_duration) << " us";
	    }

	    IntervalMetaData interval_metadata;
	    interval_metadata.interval_index = interval_index;
	    interval_metadata.round_count = round_counts[round_counts.size()/2];
	    interval_metadata.start_timeslice = start_timeslices[start_timeslices.size()/2];
	    interval_metadata.start_time = median_start_time;
	    interval_metadata.interval_duration = median_round_duration*interval_metadata.round_count;

	    actual_interval_start_time_info_.add(interval_index, interval_metadata);

	    last_completed_interval_ = interval_index;


	    /// LOGGING
	    if (ConstVariables::ENABLE_LOGGING){
		min_max_interval_start_time_log_.insert(std::pair<uint64_t, std::pair<int64_t, int64_t> >(interval_index, std::pair<int64_t, int64_t>(
			std::chrono::duration_cast<std::chrono::milliseconds>(min_start_time - compute_MPI_time_).count(),
			std::chrono::duration_cast<std::chrono::milliseconds>(max_start_time - compute_MPI_time_).count())));
		min_max_interval_duration_log_.insert(std::pair<uint64_t, std::pair<int64_t, int64_t> >(interval_index, std::pair<int64_t, int64_t>(
			round_durations[0]*interval_metadata.round_count/1000.0,round_durations[round_durations.size()-1]*interval_metadata.round_count/1000.0)));
	    }
	    /// END LOGGING
	}

	double get_mean_duration_difference(){
	    uint64_t last_completed_interval = actual_interval_start_time_info_.get_last_key();

	    // +2 because there is no proposed duration for the first two intervals
	    if (last_completed_interval < ConstVariables::SPEEDUP_SLOWDOWN_HISTORY+2)return 0;

	    double mean = 0;
	    for (uint32_t i=0 ; i< ConstVariables::SPEEDUP_SLOWDOWN_HISTORY ; i++){

		mean += std::abs((int64_t)actual_interval_start_time_info_.get(last_completed_interval-i).interval_duration - (int64_t)proposed_interval_start_time_info_.get(last_completed_interval-i).interval_duration);
	    }
	    mean /= ConstVariables::SPEEDUP_SLOWDOWN_HISTORY;

	    return mean;
	}

	uint64_t get_median_round_duration(){
	    if (actual_interval_start_time_info_.size() == 0) return 0;

	    SizedMap<uint64_t, IntervalMetaData>::iterator it = actual_interval_start_time_info_.get_end_iterator();
	    uint64_t sum_durations = 0;

	    uint32_t required_size = ConstVariables::MAX_MEDIAN_VALUES, count = 0;
	    if (actual_interval_start_time_info_.size() < required_size){
		required_size = actual_interval_start_time_info_.size();
	    }

	    do{
		--it;
		++count;
		sum_durations += (it->second.interval_duration/it->second.round_count);
	    }while (it != actual_interval_start_time_info_.get_begin_iterator() && count < required_size);

	    return sum_durations/required_size;

	}

	/// This method gets the sent time for a particular input node and timeslice
	IntervalMetaData get_interval_sent_time(uint64_t interval_index){

	    uint64_t last_completed_interval = actual_interval_start_time_info_.get_last_key();
	    IntervalMetaData last_completed_interval_info = actual_interval_start_time_info_.get(last_completed_interval);

	    uint64_t new_start_timeslice = last_completed_interval_info.start_timeslice+ last_completed_interval_info.round_count*input_node_count_ * (interval_index-last_completed_interval_info.interval_index); //TODO compute node count is NEEDED;
	    if (proposed_interval_start_time_info_.size() != 0){
		IntervalMetaData last_proposed_interval_info = proposed_interval_start_time_info_.get(proposed_interval_start_time_info_.get_last_key());
		new_start_timeslice = last_proposed_interval_info.start_timeslice+ last_proposed_interval_info.round_count*input_node_count_ * (interval_index-last_proposed_interval_info.interval_index); //TODO compute node count is NEEDED
	    }

	    uint64_t median_round_duration = get_median_round_duration();
	    uint32_t round_count = std::ceil(ConstVariables::MIN_INTERVAL_DURATION*1000000.0/(median_round_duration*1.0));
	    round_count = round_count == 0 ? 1 : round_count;

	    // LOGGING
	    interval_round_log_.insert(std::pair<uint64_t,uint32_t>(interval_index, round_count));
	    // END LOGGING


	    double mean_interval_diff = get_mean_duration_difference();

	    speedup_enabled_ = !speedup_enabled_ || (speedup_enabled_ && speedup_proposed_interval_+ ConstVariables::SPEEDUP_INTERVAL_PERIOD-1 <= interval_index) ? false : true;
	    slowdown_enabled_ = !slowdown_enabled_ || (slowdown_enabled_ && slowdown_proposed_interval_+ ConstVariables::SLOWDOWN_INTERVAL_PERIOD-1 <= interval_index) ? false : true;
	    // LOGGING
	    double enhancement_factor_log = 0;
	    // END LOGGING



	    uint64_t median_interval_duration = median_round_duration*round_count,
		    enhanced_interval_duration = median_interval_duration;

	    if (speedup_enabled_){
		if (speedup_proposed_duration_ <= median_interval_duration){
		    enhanced_interval_duration = speedup_proposed_duration_;
		    /// LOGGING
		    enhancement_factor_log = 1; // flag that median is better than prev. enhancement
		    /// END OF LOGGING
		/// LOGGING
		}else{
		    speedup_enabled_ = false;
		}
		/// END OF LOGGING
	    }
	    if (slowdown_enabled_){

		// second stage of slowing down for network relaxation
		if (slowdown_proposed_interval_+ (ConstVariables::SLOWDOWN_INTERVAL_PERIOD/2) >= interval_index &&
		    (mean_interval_diff/median_interval_duration*100) > ConstVariables::SLOWDOWN_GAP_PERCENTAGE){
		    slowdown_proposed_duration_+= ((slowdown_proposed_duration_/INTERVAL_LENGTH)*ConstVariables::SLOWDOWN_ROUND_FACTOR);
		}

		if (slowdown_proposed_duration_ <= median_interval_duration){
		    enhanced_interval_duration = slowdown_proposed_duration_;
		    /// LOGGING
		    enhancement_factor_log = -1; // flag that median is better than prev. enhancement
		    /// END OF LOGGING
		/// LOGGING
		}else{
		    slowdown_enabled_ = false;
		}
		/// END OF LOGGING
	    }

	    //speedup
	    if (!speedup_enabled_ &&
		    !slowdown_enabled_ &&
		    mean_interval_diff != 0 &&
		    (mean_interval_diff/median_interval_duration*100) <= ConstVariables::SPEEDUP_GAP_PERCENTAGE){
		enhancement_factor_log = 1;
		enhanced_interval_duration -= ((enhanced_interval_duration/INTERVAL_LENGTH)*ConstVariables::SPEEDUP_ROUND_FACTOR);
		speedup_enabled_ = true;
		speedup_proposed_duration_ = enhanced_interval_duration;
		speedup_proposed_interval_ = interval_index;

	    }

	    // slow down
	    if (!speedup_enabled_ &&
		    !slowdown_enabled_ &&
		    mean_interval_diff != 0 &&
		    speedup_proposed_interval_ != ConstVariables::MINUS_ONE && // limit the jump
		    (mean_interval_diff/median_interval_duration*100) > ConstVariables::SLOWDOWN_GAP_PERCENTAGE){

		enhancement_factor_log = -1;
		enhanced_interval_duration = speedup_proposed_duration_ + ((speedup_proposed_duration_/INTERVAL_LENGTH) * ConstVariables::SLOWDOWN_ROUND_FACTOR);

		if (enhanced_interval_duration > median_interval_duration){
		    enhancement_factor_log = -2;
		    enhanced_interval_duration = median_interval_duration;
		}else{
		    slowdown_enabled_ = true;
		    slowdown_proposed_duration_ = enhanced_interval_duration;
		    slowdown_proposed_interval_ = interval_index;
		}
	    }

	    // LOGGING
	    speedup_duration_factor_log_.insert(std::pair<uint64_t, double>(interval_index,enhancement_factor_log));
	    proposed_median_enhanced_duration_log_.insert(std::pair<uint64_t,std::pair<uint64_t,uint64_t>>(interval_index,std::pair<uint64_t,uint64_t>(median_interval_duration, enhanced_interval_duration)));
	    // END LOGGING

	    return IntervalMetaData(interval_index,round_count, new_start_timeslice,
		    last_completed_interval_info.start_time + std::chrono::microseconds(median_interval_duration * (interval_index - last_completed_interval)),
		    enhanced_interval_duration);


	   /* std::map<uint64_t, std::vector<int64_t> >::iterator it = proposed_times_log_.find(timeslice);
	    if (it == proposed_times_log_.end()){
		proposed_times_log_.insert(std::pair<uint64_t, std::vector<int64_t> >(timeslice, std::vector<int64_t>(input_node_count_)));
		it = proposed_times_log_.find(timeslice);
	    }

	    it->second[input_index] = std::chrono::duration_cast<std::chrono::microseconds>(sent_time - compute_MPI_time_).count() + sender_info_[input_index].clock_offset;
	    /// END OF Logging*/
	}

	/// This const variable limits the number of durations of timeslices to be kept
	const int32_t MAX_DURATION_HISTORY = 100;

	/// This const variable determines the interval length that a new sent_time and duration will be generated
	const uint32_t INTERVAL_LENGTH;

	/// The compute node index. The order of input nodes is based on this index
	uint64_t compute_index_;

	/// The local time of the compute node when the MPI barrier reached
	std::chrono::high_resolution_clock::time_point compute_MPI_time_;

	/// The number of input nodes which the compute receives data from
	uint32_t input_node_count_;

	/// This is a list of input nodes with the history of their data
	std::vector<InputSchedulerData> sender_info_;

	/// A history of the proposed start time for intervals <timeslice, <time, gap>>
	SizedMap<uint64_t, IntervalMetaData> proposed_interval_start_time_info_;

	SizedMap<uint64_t, IntervalMetaData> actual_interval_start_time_info_;

	/// Count of the acked contributions from input nodes <timeslice, count>
	SizedMap<uint64_t, uint32_t> acked_interval_count_;

	/// Contains the last completed interval that the scheduler receives all data about from all input nodes
	uint64_t last_completed_interval_ = ConstVariables::MINUS_ONE;

	SizedMap<uint64_t, uint64_t> sum_median_interval_duration_;

	bool speedup_enabled_ = false;
	uint64_t speedup_proposed_duration_;
	uint64_t speedup_proposed_interval_ = ConstVariables::MINUS_ONE;

	bool slowdown_enabled_ = false;
	uint64_t slowdown_proposed_duration_;
	uint64_t slowdown_proposed_interval_ = ConstVariables::MINUS_ONE;


	//std::set<uint64_t> actual_grouped_durations_;


	/// LOGGING
	std::map<uint64_t, std::pair<int64_t, int64_t> > min_max_interval_start_time_log_;
	std::map<uint64_t, std::pair<int64_t, int64_t> > min_max_interval_duration_log_;
	std::map<uint64_t, std::pair<uint64_t, uint64_t> > proposed_median_enhanced_duration_log_;
	std::map<uint64_t, double > speedup_duration_factor_log_;
	std::map<uint64_t, uint32_t > interval_round_log_;

	/*
	// timeslice, [{proposed, actual}]
	std::map<uint64_t, std::vector<std::pair<int64_t, int64_t> > > proposed_actual_times_log_;
	///
	std::map<uint64_t, uint64_t> durations_log_;
	///
	std::map<uint64_t, std::vector<int64_t> > proposed_times_log_;
	/// interval index, <taken duration, proposed duration gap>
	std::map<uint64_t, std::pair<uint64_t, uint64_t> > interval_duration_log_;
	*/

};


} // namespace tl_libfabric

