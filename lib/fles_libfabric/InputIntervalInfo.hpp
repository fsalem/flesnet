// Copyright 2017 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "SizedMap.hpp"
#include <chrono>
#include <utility>

#pragma pack(1)

namespace tl_libfabric
{
struct InputIntervalInfo {
    uint64_t index;
    uint64_t start_ts;
    uint64_t end_ts;
    std::chrono::high_resolution_clock::time_point proposed_start_time;
    uint64_t proposed_duration;
    std::chrono::high_resolution_clock::time_point actual_start_time;
    uint64_t actual_duration;

    uint64_t count_sent_ts = ConstVariables::ZERO;

    uint64_t count_acked_ts = ConstVariables::ZERO;

    uint64_t count_rounds = ConstVariables::ZERO;

    bool cb_blocked = false;
    std::chrono::high_resolution_clock::time_point cb_blocked_start_time;
    uint64_t cb_blocked_duration = 0;

    bool ib_blocked = false;
    std::chrono::high_resolution_clock::time_point ib_blocked_start_time;
    uint64_t ib_blocked_duration = 0;

    uint64_t get_duration_to_next_round(){
	init_statistical_variables();

	if (duration_per_ts == 0) return ConstVariables::ZERO;
	// If the proposed finish time is reached, send as fast as possible.
	if ((proposed_start_time + std::chrono::microseconds(proposed_duration)) > std::chrono::high_resolution_clock::now())return ConstVariables::ZERO;

	uint64_t expected_sent_ts = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - actual_start_time).count() / duration_per_ts;

	if (expected_sent_ts <= count_sent_ts){ /// sending faster than proposed
	    return duration_per_round;
	}
	if (expected_sent_ts - count_sent_ts > num_ts_per_round){ // scheduler is at one round behind
	    return ConstVariables::ZERO;
	}
	return duration_per_round - ((expected_sent_ts - count_sent_ts) * duration_per_ts);
    }

    uint64_t get_current_round_index(){
	init_statistical_variables();
    	return (uint64_t)(count_sent_ts / num_ts_per_round);
    }

    bool is_ts_within_current_round(uint64_t ts){
	return ts < (get_current_round_index() * num_ts_per_round) + num_ts_per_round + start_ts && ts <= end_ts;
    }

    bool is_interval_sent_completed(){
	return count_sent_ts == (end_ts-start_ts+1) ? true: false;
    }

    bool is_interval_sent_ack_completed(){
	return is_interval_sent_completed() && (count_acked_ts*1.0)/(count_sent_ts*1.0) >= 0.7 ? true: false;
    }

private:

    void init_statistical_variables(){
	if (duration_per_ts == ConstVariables::ZERO || duration_per_round == ConstVariables::ZERO){
    	    duration_per_ts = proposed_duration / (end_ts - start_ts + 1);
    	    duration_per_round = proposed_duration/ConstVariables::SCHEDULER_INTERVAL_LENGTH;
    	    num_ts_per_round = (end_ts - start_ts + 1) / ConstVariables::SCHEDULER_INTERVAL_LENGTH;
    	}
    }
    uint64_t duration_per_ts = ConstVariables::ZERO;
    uint64_t duration_per_round = ConstVariables::ZERO;
    uint64_t num_ts_per_round = ConstVariables::ZERO;
};
}

#pragma pack()