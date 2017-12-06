// Copyright 2014 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "SizedMap.hpp"
#include <chrono>
#include <utility>

#pragma pack(1)

namespace tl_libfabric
{
struct InputSchedulerData {
    //uint32_t index_;
    std::chrono::high_resolution_clock::time_point MPI_Barrier_time;
    int64_t clock_offset;
    /// <interval index, <actual_start_time,duration>>. Duration is the spent time from sending the contribution till getting the acknowledgement
    SizedMap< uint64_t, std::pair< std::chrono::high_resolution_clock::time_point, uint64_t > > interval_info_;
    // TODO add a list of durations to simplify the median calculations
    uint64_t median_duration = ConstVariables::ZERO;
    uint64_t max_duration = ConstVariables::ZERO;
    uint64_t min_duration = ConstVariables::ZERO;
};
}

#pragma pack()
