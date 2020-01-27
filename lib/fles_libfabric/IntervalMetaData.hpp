// Copyright 2018 Farouk Salem <salem@zib.de>

#pragma once

#include "ConstVariables.hpp"
#include <chrono>
#include <vector>
#include <assert.h>

#pragma pack(1)

namespace tl_libfabric
{
/// Structure representing the meta-data of intervals that is shared between inpute and compute nodes
/// input channel.
struct IntervalMetaData {
    /// The interval index
    uint64_t interval_index = ConstVariables::MINUS_ONE;

    /// The interval's rounds
    uint32_t round_count = 1;

    /// Start timeslice
    uint64_t start_timeslice = ConstVariables::MINUS_ONE;

    /// Last timeslice
    uint64_t last_timeslice = ConstVariables::MINUS_ONE;

    // The start time for the interval [The actual time when a input node is the sender, the proposed time when a compute node is the sender]
    std::chrono::high_resolution_clock::time_point start_time;

    // duration for the whole interval
    uint64_t interval_duration;

    // This list holds the frequency distribution on compute nodes when Compute Scheduler sends it
    // It holds the sum of blockage time when Input scheduler sends
    uint64_t compute_nodes_distribution[ConstVariables::MAX_COMPUTE_NODE_COUNT];

    // The number of compute nodes (active and timeout)
    uint32_t compute_node_count;

    IntervalMetaData(){}

    IntervalMetaData(uint64_t index, uint32_t rounds, uint64_t start_ts, uint64_t last_ts,
		    std::chrono::high_resolution_clock::time_point start_time,
		    uint64_t duration, std::vector<uint64_t> compute_node_dist):
		    interval_index(index), round_count(rounds), start_timeslice(start_ts),
		    last_timeslice(last_ts), start_time(start_time), interval_duration(duration){
	compute_node_count = compute_node_dist.size();
	assert (compute_node_count <= ConstVariables::MAX_COMPUTE_NODE_COUNT);
	for (uint32_t i=0 ; i<compute_node_count ; i++)compute_nodes_distribution[i] = compute_node_dist[i];
    }
};
}

#pragma pack()
