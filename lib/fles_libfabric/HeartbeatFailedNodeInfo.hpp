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
    uint64_t last_completed_desc = ConstVariables::MINUS_ONE;
    // When this info is sent out from:
    // (1) an input process: the timeslice that other compute nodes will be
    // blocked starting from it (2) a compute process: the last timeslice to be
    // sent before distributing the contributions of the failed node
    uint64_t timeslice_trigger = ConstVariables::MINUS_ONE;

    HeartbeatFailedNodeInfo() {}
    HeartbeatFailedNodeInfo(uint32_t index) : index(index) {}
    HeartbeatFailedNodeInfo(uint32_t index, uint64_t desc, uint64_t trigger)
        : index(index), last_completed_desc(desc), timeslice_trigger(trigger)
    {
    }
    bool operator<(const HeartbeatFailedNodeInfo& right) const
    {
        return index < right.index;
    }

    bool operator>(const HeartbeatFailedNodeInfo& right) const
    {
        return index > right.index;
    }

    bool operator==(const HeartbeatFailedNodeInfo& right) const
    {
        return index == right.index;
    }
};
} // namespace tl_libfabric
#pragma pack()
