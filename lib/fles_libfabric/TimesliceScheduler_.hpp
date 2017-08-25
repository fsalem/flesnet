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

namespace tl_libfabric {

class TimesliceScheduler {
public:
	TimesliceScheduler(const uint64_t compute_index,
		const uint32_t input_node_count):
			compute_index_(compute_index), input_node_count_(input_node_count),
			ts_duration_(MAX_DURATION_HISTORY), acked_ts_count_(MAX_DURATION_HISTORY){

		for (uint_fast16_t i=0 ; i<input_node_count_ ; i++)
			sender_info_.push_back(InputSchedulerData());
		//sender_info_.resize(input_node_count_);
		assert(sender_info_.size() == input_node_count_);
		completed_ts_ = false;
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
	void init_input_index_info(uint64_t input_index,
			std::chrono::high_resolution_clock::time_point MPI_time){

		assert(sender_info_.size() == input_node_count_);
		sender_info_[input_index].MPI_Barrier_time = MPI_time;
		sender_info_[input_index].clock_offset = std::chrono::duration_cast
				<std::chrono::microseconds>(compute_MPI_time_ - MPI_time).count();
	}

	/// This method adds the received information from an input node to the scheduler data
	void add_input_ts_info(uint64_t input_index, uint64_t timeslice,
			std::chrono::high_resolution_clock::time_point sent_time,
			std::chrono::high_resolution_clock::time_point proposed_time,
			double duration){

	    if (!sender_info_[input_index].ts_sent_info_.contains(timeslice)) {
		    sender_info_[input_index].ts_sent_info_.add(timeslice, std::pair<std::chrono::high_resolution_clock::time_point,uint64_t>(sent_time, duration));
		    increament_acked_ts(timeslice);
		    // TODO logging!

		    std::map<uint64_t, std::vector<std::pair<int64_t, int64_t> > >::iterator it = proposed_actual_times_log_.find(timeslice);

		    if (it == proposed_actual_times_log_.end()){
			proposed_actual_times_log_.insert(std::pair<uint64_t,
				std::vector<std::pair<int64_t, int64_t> > >(timeslice, std::vector<std::pair<int64_t, int64_t> >(input_node_count_)));
			it = proposed_actual_times_log_.find(timeslice);
		    }

		    it->second[input_index] = std::make_pair<int64_t, int64_t>(std::chrono::duration_cast<std::chrono::microseconds>(proposed_time - compute_MPI_time_).count() + sender_info_[input_index].clock_offset,
			std::chrono::duration_cast<std::chrono::microseconds>(sent_time - compute_MPI_time_).count() + sender_info_[input_index].clock_offset);

		    if (it->second[input_index].first < 0 )it->second[input_index].first = 0;
	    }

	}

	/// This method gets the sent time for a particular input node and timeslice
	std::chrono::high_resolution_clock::time_point get_sent_time(
		    uint64_t input_index, uint64_t timeslice){

	    uint64_t last_complete_ts = get_last_complete_ts();
	    uint64_t last_complete_ts_duration = ts_duration_.get(last_complete_ts);
	    // get last sent time of the received contribution of the last complete timeslice
	    std::chrono::high_resolution_clock::time_point last_received_contribution_time =
			    sender_info_[(compute_index_ - 1) % input_node_count_].ts_sent_info_.get(
					    last_complete_ts).first
					    + std::chrono::microseconds(
							    sender_info_[(compute_index_ - 1)
									    % input_node_count_].clock_offset);
	    uint64_t sum_needed_duration = 0;
	    for (uint32_t input_node = compute_index_; input_node != compute_index_;
			    ++input_node % input_node_count_) {
		    // TODO to add alpha
		    sum_needed_duration += sender_info_[input_node].ts_sent_info_.get(last_complete_ts).second;
	    }

	    std::chrono::high_resolution_clock::time_point sent_time = last_received_contribution_time + std::chrono::microseconds(
			    sum_needed_duration - sender_info_[input_index].clock_offset);

	    for (uint64_t ts = last_complete_ts+input_node_count_ ; ts<timeslice ; ts+=input_node_count_){
		    sent_time += std::chrono::microseconds(last_complete_ts_duration);
	    }

	    return sent_time;
	}

	/// This method gets the duration needed for receiving a complete timeslice after a specific timeslice
	uint64_t get_ts_duration(uint64_t timeslice){

	    if (ts_duration_.contains(timeslice)) return ts_duration_.get(timeslice);

	    return ConstVariables::MINUS_ONE;
	}

	///This method returns the latest completed timeslice
	uint64_t get_last_complete_ts() {

	    if (ts_duration_.size() == 0) {
		    return ConstVariables::MINUS_ONE;
	    }
	    return ts_duration_.get_last_key();
	}

	bool check_new_ts_completed(){

	    if (completed_ts_) {
		    completed_ts_ = false;
		    return true;
	    }
	    return false;
	}

	void build_ack_time_file(){

	}

	void build_scheduled_time_file(){

	    std::ofstream log_file;
	    log_file.open(std::to_string(compute_index_)+".compute.proposed_vs_sent_time.out");


	    log_file << std::setw(25) << "Input Index" << std::setw(25) << "Timeslice" << std::setw(25) << "Contribution" << std::setw(25) << "Proposed(t)" << std::setw(25) << "Sent(t)" << std::setw(25) << "Diff" << std::setw(25) << "Duration" << "\n";

	    std::map<uint64_t, std::vector<std::pair<int64_t, int64_t> > >::iterator it = proposed_actual_times_log_.begin();
	    std::vector<std::pair<int64_t, int64_t>> times;
	    while (it != proposed_actual_times_log_.end()){
		times = it->second;
		for (uint32_t i=0 ; i<times.size() ; i++){
		    log_file << std::setw(25) << i << std::setw(25) << it->first << std::setw(25) << (it->first+i) << std::setw(25) << (times[i].first*1.0)/1000.0 << std::setw(25) << (times[i].second*1.0)/1000.0 << std::setw(25) << ((times[i].second - times[i].first)*1.0)/1000.0 << std::setw(25) << (durations_log_.find(it->first)->second*1.0)/1000.0 << "\n";
		}

		log_file.flush();
		++it;
	    }


	    log_file.close();
	}

private:

	/// This increases the counter for the received timeslices to trigger when to start calculate the sent time
	void increament_acked_ts(uint64_t timeslice) {

	    uint32_t count = 1;
	    if (acked_ts_count_.contains(timeslice)) {
		count = acked_ts_count_.get(timeslice)+1;
		acked_ts_count_.update(timeslice,count);
	    } else {
		acked_ts_count_.add(timeslice, count);
	    }
	    if (count == input_node_count_) {
		calculate_total_ts_duration(timeslice);
	    }
	}

	/// This calculates the needed duration to receive a complete timeslice from all input nodes
	void calculate_total_ts_duration(uint64_t timeslice){

	    uint64_t total_duration = 0;
	    for (uint32_t i = 0; i < input_node_count_; i++) {
		    total_duration += sender_info_[i].ts_sent_info_.get(timeslice).second;
	    }
	    ts_duration_.add(timeslice, total_duration);
	    durations_log_.insert(std::pair<uint64_t, uint64_t>(timeslice, total_duration));
	    completed_ts_ = true;
	    // TODO do statistics
	    // TODO add +- theta
	}

	/// This const variable limits the number of durations of timeslices to be kept
	const int32_t MAX_DURATION_HISTORY = 100;

	/// The compute node index. The order of input nodes is based on this index
	uint64_t compute_index_;

	/// The local time of the compute node when the MPI barrier reached
	std::chrono::high_resolution_clock::time_point compute_MPI_time_;

	/// The number of input nodes which the compute receives data from
	uint32_t input_node_count_;

	/// This is a list of input nodes with the history of their data
	std::vector<InputSchedulerData> sender_info_;

	/// A history of the estimated durations <timeslice, duration>
	SizedMap<uint64_t, uint64_t> ts_duration_;

	/// Count of the acked contributions from input nodes <timeslice, count>
	SizedMap<uint64_t, uint32_t> acked_ts_count_;

	/// Triggers if there are new completed timeslices
	bool completed_ts_ = false;


	/// LOGGING
	// timeslice, [{proposed, actual}]
	std::map<uint64_t, std::vector<std::pair<int64_t, int64_t> > > proposed_actual_times_log_;
	///
	std::map<uint64_t, uint64_t> durations_log_;

};


} // namespace tl_libfabric
