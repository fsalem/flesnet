// Copyright 2012-2013 Jan de Cuveland <cmail@cuveland.de>
// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#pragma once

#pragma pack(1)

namespace tl_libfabric
{
struct HeartbeatFailedNodeInfo {
    // Index of the
    uint32_t index = ConstVariables::MINUS_ONE;
    // Last completed descriptor of this failed node
    uint64_t last_completed_desc = ConstVariables::ZERO;
    // When this info is sent out from:
    // (1) an input process: the timeslice that other compute nodes will be blocked starting from it
    // (2) a compute process: the last timeslice to be sent before distributing the contributions of the failed node
    uint64_t timeslice_trigger = ConstVariables::ZERO;
};
}
#pragma pack()
