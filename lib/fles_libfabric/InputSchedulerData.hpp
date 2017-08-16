// Copyright 2014 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#include "InputNodeInfo.hpp"
#include "ConstVariables.hpp"
#include <map>
#include <chrono>

#pragma pack(1)

namespace tl_libfabric
{
struct InputSchedulerData {
	//uint32_t index_;
	std::chrono::high_resolution_clock::time_point MPI_Barrier_time;
	uint64_t clock_offset;
	/// <timeslice number, <sent_time,duration>>. Duration is the spent time from sending the contribution till getting the acknowledgement
	std::map<uint64_t,std::pair<std::chrono::high_resolution_clock::time_point,uint64_t>> ts_sent_info_;

};
}

#pragma pack()
