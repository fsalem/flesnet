// Copyright 2014 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "SizedMap.hpp"
#include "SizedSet.hpp"
#include <chrono>
#include <utility>

#pragma pack(1)

namespace tl_libfabric
{
struct InputSchedulerData {
    //uint32_t index_;
    std::chrono::system_clock::time_point MPI_Barrier_time;
    int64_t clock_offset = 0;
    /// <interval index, <actual_start_time,duration>>. Duration is the spent time from sending the contribution till getting the acknowledgement
    SizedMap< uint64_t, IntervalMetaData> interval_info_;
    SizedSet<uint64_t> round_durations;

    InputSchedulerData():round_durations(ConstVariables::MAX_MEDIAN_VALUES){}
};
}

#pragma pack()
