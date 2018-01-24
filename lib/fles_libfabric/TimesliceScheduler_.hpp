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
	void add_input_interval_info(uint32_t input_index, uint64_t interval_index,
			std::chrono::high_resolution_clock::time_point actual_start_time,
			std::chrono::high_resolution_clock::time_point proposed_start_time,
			double interval_duration){

	    if (!sender_info_[input_index].interval_info_.contains(interval_index)) {
		    sender_info_[input_index].interval_info_.add(interval_index, std::pair<std::chrono::high_resolution_clock::time_point,uint64_t>(actual_start_time + std::chrono::microseconds(sender_info_[input_index].clock_offset), interval_duration));

		    if (!sum_median_interval_duration_.contains(interval_index)){
			sum_median_interval_duration_.add(interval_index, ConstVariables::ZERO);
		    }
		    SizedMap<uint64_t, uint64_t>::iterator sum_it = sum_median_interval_duration_.get_iterator(interval_index);

		    if (sender_info_[input_index].median_duration == ConstVariables::ZERO){
			sender_info_[input_index].median_duration = interval_duration;
		    }else{
			sender_info_[input_index].median_duration = calculate_median_intervals_duration(input_index);
		    }

		    if (sender_info_[input_index].min_duration == ConstVariables::ZERO || sender_info_[input_index].min_duration > interval_duration){
			sender_info_[input_index].min_duration = interval_duration;
		    }

		    if (sender_info_[input_index].max_duration == ConstVariables::ZERO || sender_info_[input_index].max_duration < interval_duration){
			sender_info_[input_index].max_duration = interval_duration;
		    }

		    sum_it->second += sender_info_[input_index].median_duration;
		    increament_acked_interval(interval_index);

		    /*/// Logging
		    if (ConstVariables::ENABLE_LOGGING){
			std::map<uint64_t, std::vector<std::pair<int64_t, int64_t> > >::iterator it = proposed_actual_times_log_.find(interval_index);

			if (it == proposed_actual_times_log_.end()){
			    proposed_actual_times_log_.insert(std::pair<uint64_t,
				    std::vector<std::pair<int64_t, int64_t> > >(interval_index, std::vector<std::pair<int64_t, int64_t> >(input_node_count_)));
			    it = proposed_actual_times_log_.find(interval_index);
			}

			it->second[input_index] = std::make_pair<int64_t, int64_t>(std::chrono::duration_cast<std::chrono::microseconds>(proposed_start_time - compute_MPI_time_).count() + sender_info_[input_index].clock_offset,
			    std::chrono::duration_cast<std::chrono::microseconds>(actual_start_time - compute_MPI_time_).count() + sender_info_[input_index].clock_offset);

			if (it->second[input_index].first < 0 )it->second[input_index].first = 0;
		    }
		    /// END OF Logging*/
	    }

	}

	/// This method retrieves the interval information that will be sent to input nodes
	std::pair<std::chrono::high_resolution_clock::time_point, uint64_t> get_interval_info(uint64_t interval_index,uint32_t input_index){
	    std::pair<std::chrono::high_resolution_clock::time_point, uint64_t> interval_info;
	    if (proposed_interval_start_time_info_.contains(interval_index)){
		interval_info = proposed_interval_start_time_info_.get(interval_index);
	    }else{
		interval_info = get_interval_sent_time(interval_index);
		proposed_interval_start_time_info_.add(interval_index, interval_info);
	    }
	    interval_info.first -= std::chrono::microseconds(sender_info_[input_index].clock_offset);
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
			<< std::setw(25) << "Speedup Factor"<< "\n";

		std::map<uint64_t, std::pair<int64_t, int64_t> >::iterator times_it = min_max_interval_start_time_log_.begin(),
			dur_it = min_max_interval_duration_log_.begin();
		std::map<uint64_t, double>::iterator speedup_factor;

		while (times_it != min_max_interval_start_time_log_.end() && dur_it != min_max_interval_duration_log_.end()){
		    double factor = 0.0;
		    speedup_factor = speedup_duration_factor_log_.find(times_it->first);
		    if (speedup_factor != speedup_duration_factor_log_.end())
			factor = speedup_factor->second;

		    log_file << std::setw(25) << times_it->first
			    << std::setw(25) << times_it->second.first
			    << std::setw(25) << times_it->second.second
			    << std::setw(25) << dur_it->second.first
			    << std::setw(25) << dur_it->second.second
			    << std::setw(25) << factor << "\n";
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
	    if (true){
		std::ofstream log_file;
		log_file.open(std::to_string(compute_index_)+".compute.mean_variance_interval.out");

		log_file << std::setw(25) << "Interval"
			<< std::setw(25) << "Mean"
			<< std::setw(25) << "\"Std. Deviation\"" << "\n";

		std::map<uint64_t, std::pair<double, double> >::iterator duration_it = mean_varience_interval_log_.begin();

		while (duration_it != mean_varience_interval_log_.end()){

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

	/// This struct contains the needed data for update theta and alpha. It contains the variance, median, and mean of set of durations
	struct TimeSchedulerStatsData {
	    uint64_t mean = 0;
	    uint64_t median = 0;
	    uint64_t variance = 0;
	};

	/*uint32_t get_timeslice_interval(uint64_t timeslice){
	    // TODO INTERVAL_LENGTH * num_input_nodes should be INTERVAL_LENGTH * num_COMPUTE_nodes
	    return(timeslice / (INTERVAL_LENGTH * input_node_count_)); // fraction will be thrown away
	}*/
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

	/*uint64_t get_majority_median_duration(){
	    uint64_t last_completed_interval = actual_interval_start_time_info_.get_last_key();

	    if (last_completed_interval >= ConstVariables::SPEEDUP_HISTORY){

		std::map<uint64_t,uint16_t> duration_count;
		std::vector<uint64_t> last_durations(ConstVariables::SPEEDUP_HISTORY);
		uint64_t majority_duration = actual_interval_start_time_info_.get(last_completed_interval).second;
		uint16_t majority_count = 1;

		uint64_t dur;
		std::map<uint64_t,uint16_t>::iterator it;

		for (uint32_t i=0 ; i< ConstVariables::SPEEDUP_HISTORY ; i++){
		    dur = actual_interval_start_time_info_.get(last_completed_interval-i).second;
		    it = duration_count.find(dur);
		    if (it == duration_count.end()){
			duration_count.insert(std::pair<uint64_t,uint16_t>(dur,1));
		    }else{
			++it->second;
			if (it->second > majority_count){
			    majority_duration = it->first;
			    majority_count = it->second;
			}
		    }
		}

		// TODO 0.6 to be configurable
		if (majority_count >= 0.6 * ConstVariables::SPEEDUP_HISTORY)
		    return majority_duration;
	    }
	    return 0;
	}*/

	/// This calculates the needed duration to complete an interval from all input nodes
	void calculate_interval_info(uint64_t interval_index){

	    std::chrono::high_resolution_clock::time_point min_start_time, max_start_time, median_start_time, tmp;
	    std::vector<uint64_t> interval_durations;

	    // get the earliest start time!
	    for (uint32_t indx = 0; indx < input_node_count_ ; indx++){
		if (interval_durations.empty()){
		    min_start_time = sender_info_[indx].interval_info_.get(interval_index).first;
		    max_start_time = min_start_time;
		}
		interval_durations.push_back(sender_info_[indx].interval_info_.get(interval_index).second);
		tmp = sender_info_[indx].interval_info_.get(interval_index).first;
		if (tmp < min_start_time){
		    min_start_time = tmp;
		}

		if (tmp > max_start_time){
		    max_start_time = tmp;
		}
	    }
	    std::sort(interval_durations.begin(),interval_durations.end());
	    uint64_t median_interval_duration;
	    if (interval_durations.size() >= 2 && interval_durations.size() % 2 == 0){
		int mid_index = interval_durations.size()/2;
		median_interval_duration = (interval_durations[mid_index-1] + interval_durations[mid_index])/2;
	    }else{
		median_interval_duration = interval_durations[interval_durations.size()/2];
	    }

	    median_start_time = min_start_time + std::chrono::microseconds(std::chrono::duration_cast<std::chrono::microseconds>(max_start_time - min_start_time).count()/2);
	    if (false){
		L_(info) << "[" << compute_index_ << "] interval "
			<< interval_index << " took "
			<< (median_interval_duration) << " us";
	    }

	    actual_interval_start_time_info_.add(interval_index, std::make_pair(median_start_time, median_interval_duration));
	    last_completed_interval_ = interval_index;

	    /// SPEEDUP Calculations
	    /*uint64_t majority_duration = get_majority_median_duration();
	    if (majority_duration != 0 && actual_grouped_durations_.count(majority_duration) == 0)
		actual_grouped_durations_.insert(majority_duration);
*/
	    /// END

	    /// LOGGING
	    if (ConstVariables::ENABLE_LOGGING){
		min_max_interval_start_time_log_.insert(std::pair<uint64_t, std::pair<int64_t, int64_t> >(interval_index, std::pair<int64_t, int64_t>(
			std::chrono::duration_cast<std::chrono::milliseconds>(min_start_time - compute_MPI_time_).count(),
			std::chrono::duration_cast<std::chrono::milliseconds>(max_start_time - compute_MPI_time_).count())));
		min_max_interval_duration_log_.insert(std::pair<uint64_t, std::pair<int64_t, int64_t> >(interval_index, std::pair<int64_t, int64_t>(
			interval_durations[0]/1000.0,interval_durations[interval_durations.size()-1]/1000.0)));
	    }
	    /// END LOGGING
	}

	std::pair<double, double> get_mean_variance(){
	    uint64_t last_completed_interval = actual_interval_start_time_info_.get_last_key();

	    // +2 because there is no proposed duration for the first two intervals
	    if (last_completed_interval < ConstVariables::SPEEDUP_SLOWDOWN_HISTORY+2)return std::pair<double,double>(0,0);

	    double mean = 0, variance = 0;
	    int32_t diff[ConstVariables::SPEEDUP_SLOWDOWN_HISTORY];
	    for (uint32_t i=0 ; i< ConstVariables::SPEEDUP_SLOWDOWN_HISTORY ; i++){
		diff[i] = actual_interval_start_time_info_.get(last_completed_interval-i).second - proposed_interval_start_time_info_.get(last_completed_interval-i).second;
		if (diff[i] < 0)diff[i]*=-1;
		mean += diff[i];
	    }
	    mean /= ConstVariables::SPEEDUP_SLOWDOWN_HISTORY;

	    for (uint32_t i=0 ; i< ConstVariables::SPEEDUP_SLOWDOWN_HISTORY ; i++){
		variance += std::pow(diff[i]-mean,2);
	    }
	    variance /= (ConstVariables::SPEEDUP_SLOWDOWN_HISTORY-1);

	    return std::pair<double, double>(mean, std::sqrt(variance));
	}

	/// This method gets the sent time for a particular input node and timeslice
	std::pair<std::chrono::high_resolution_clock::time_point, uint64_t> get_interval_sent_time(uint64_t interval_index){

	    uint64_t last_completed_interval = actual_interval_start_time_info_.get_last_key();
	    std::pair<std::chrono::high_resolution_clock::time_point,uint64_t> interval_info = actual_interval_start_time_info_.get(last_completed_interval);
	    uint64_t median_interval_duration = sum_median_interval_duration_.get(last_completed_interval)/input_node_count_;

	    std::pair<double, double> stats_data = get_mean_variance();
	    /// LOGGING
	    if (stats_data.first != 0){
		mean_varience_interval_log_.insert(std::pair<uint64_t,std::pair<double,double>>(proposed_interval_start_time_info_.get_last_key(),stats_data));
	    }
	    /// END OF LOGGING

	    speedup_enabled_ = !speedup_enabled_ || (speedup_enabled_ && speedup_proposed_interval_+ ConstVariables::MAX_MEDIAN_VALUES-1 <= interval_index) ? false : true;
	    slowdown_enabled_ = !slowdown_enabled_ || (slowdown_enabled_ && slowdown_proposed_interval_+ ConstVariables::MAX_MEDIAN_VALUES-1 <= interval_index) ? false : true;
	    double enhancement_factor = 1;
	    uint64_t enhanced_interval_duration = median_interval_duration;

	    if (speedup_enabled_){
		if (speedup_proposed_duration_ <= median_interval_duration){
		    enhanced_interval_duration = speedup_proposed_duration_;
		    /// LOGGING
		    enhancement_factor = 5; // flag that median is better than prev. enhancement
		    /// END OF LOGGING
		/// LOGGING
		}else{
		    //enhanced_interval_duration = median_interval_duration;
		    enhancement_factor = 10; // flag that median is better than prev. enhancement
		}
		/// END OF LOGGING
	    }
	    if (slowdown_enabled_){
		if (slowdown_proposed_duration_ <= median_interval_duration){
		    enhanced_interval_duration = slowdown_proposed_duration_;
		    /// LOGGING
		    enhancement_factor = -5; // flag that median is better than prev. enhancement
		    /// END OF LOGGING
		/// LOGGING
		}else{
		    //enhanced_interval_duration = median_interval_duration;
		    enhancement_factor = -10; // flag that median is better than prev. enhancement
		}
		/// END OF LOGGING

		// second stage of slowing down for network relaxation
		if (slowdown_proposed_interval_+ (ConstVariables::MAX_MEDIAN_VALUES/2) == interval_index){
		    slowdown_proposed_duration_*= ConstVariables::SLOWDOWN_FACTOR;
		}
	    }

	    //speedup
	    if (!speedup_enabled_ &&
		    !slowdown_enabled_ &&
		    stats_data.first != 0 &&
		    (stats_data.first/median_interval_duration*100) <= ConstVariables::SPEEDUP_GAP_PERCENTAGE){
		enhancement_factor = ConstVariables::SPEEDUP_FACTOR;
		enhanced_interval_duration *= enhancement_factor;
		speedup_enabled_ = 1;
		speedup_proposed_duration_ = enhanced_interval_duration;
		speedup_proposed_interval_ = interval_index;

	    }

	    // slow down
	    if (!speedup_enabled_ &&
		    !slowdown_enabled_ &&
		    stats_data.first != 0 &&
		    (stats_data.first/median_interval_duration*100) > ConstVariables::SLOWDOWN_GAP_PERCENTAGE){
		enhancement_factor = ConstVariables::SLOWDOWN_FACTOR;

		// limit the jump
		if (speedup_proposed_interval_ != ConstVariables::MINUS_ONE)
		    enhanced_interval_duration = speedup_proposed_duration_ * enhancement_factor;
		else enhanced_interval_duration *= enhancement_factor;

		slowdown_enabled_ = 1;
		slowdown_proposed_duration_ = enhanced_interval_duration;
		slowdown_proposed_interval_ = interval_index;
	    }

	    if (false){
		L_(info) << "[" << compute_index_ << "] last complete interval: "
			<< last_completed_interval << " with duration "
			<< interval_info.second << " us and the requested interval: "
			<< interval_index << " with duration "
			<< enhanced_interval_duration;
	    }

	    // LOGGING
	    speedup_duration_factor_log_.insert(std::pair<uint64_t, double>(interval_index,enhancement_factor));
	    // END LOGGING

	    proposed_median_enhanced_duration_log_.insert(std::pair<uint64_t,std::pair<uint64_t,uint64_t>>(interval_index,std::pair<uint64_t,uint64_t>(median_interval_duration, enhanced_interval_duration)));

	    return std::pair<std::chrono::high_resolution_clock::time_point, uint64_t>(
		    interval_info.first + std::chrono::microseconds(median_interval_duration * (interval_index - last_completed_interval)),
		    enhanced_interval_duration);


	   /* std::map<uint64_t, std::vector<int64_t> >::iterator it = proposed_times_log_.find(timeslice);
	    if (it == proposed_times_log_.end()){
		proposed_times_log_.insert(std::pair<uint64_t, std::vector<int64_t> >(timeslice, std::vector<int64_t>(input_node_count_)));
		it = proposed_times_log_.find(timeslice);
	    }

	    it->second[input_index] = std::chrono::duration_cast<std::chrono::microseconds>(sent_time - compute_MPI_time_).count() + sender_info_[input_index].clock_offset;
	    /// END OF Logging*/
	}

	uint64_t calculate_median_intervals_duration(uint32_t input_index){

	    if (sender_info_[input_index].interval_info_.size() == 0)return ConstVariables::MINUS_ONE;

	    // TODO SHOULD BE ENHANZED!!! BRUTEFORCE CODE!
	    SizedMap< uint64_t, std::pair< std::chrono::high_resolution_clock::time_point, uint64_t > >::iterator
		    start_it = sender_info_[input_index].interval_info_.get_begin_iterator(),
		    end_it = sender_info_[input_index].interval_info_.get_end_iterator();
	    std::vector<uint64_t> durations;

	    int max_count = ConstVariables::MAX_MEDIAN_VALUES;
	    while (max_count-- > 0 && --end_it != start_it){
		durations.push_back(end_it->second.second);
	    }
	    std::sort(durations.begin(), durations.end());
	    if (durations.size() > 1){
		int mid_index = durations.size()/2;
		return (durations[mid_index-1]+durations[mid_index])/2;
	    }
	    return durations[durations.size()/2];
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
	SizedMap<uint64_t, std::pair<std::chrono::high_resolution_clock::time_point,uint64_t>> proposed_interval_start_time_info_;

	SizedMap<uint64_t, std::pair<std::chrono::high_resolution_clock::time_point,uint64_t>> actual_interval_start_time_info_;

	/// Count of the acked contributions from input nodes <timeslice, count>
	SizedMap<uint64_t, uint32_t> acked_interval_count_;

	/// Contains the last completed interval that the scheduler receives all data about from all input nodes
	uint64_t last_completed_interval_ = ConstVariables::MINUS_ONE;

	SizedMap<uint64_t, uint64_t> sum_median_interval_duration_;

	bool speedup_enabled_ = 0;
	uint64_t speedup_proposed_duration_;
	uint64_t speedup_proposed_interval_ = ConstVariables::MINUS_ONE;

	bool slowdown_enabled_ = 0;
	uint64_t slowdown_proposed_duration_;
	uint64_t slowdown_proposed_interval_ = ConstVariables::MINUS_ONE;


	//std::set<uint64_t> actual_grouped_durations_;


	/// LOGGING
	std::map<uint64_t, std::pair<int64_t, int64_t> > min_max_interval_start_time_log_;
	std::map<uint64_t, std::pair<int64_t, int64_t> > min_max_interval_duration_log_;
	std::map<uint64_t, std::pair<uint64_t, uint64_t> > proposed_median_enhanced_duration_log_;
	std::map<uint64_t, std::pair<double, double> > mean_varience_interval_log_;
	std::map<uint64_t, double > speedup_duration_factor_log_;

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

