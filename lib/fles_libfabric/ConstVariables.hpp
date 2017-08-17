// Copyright 2017 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>


#pragma once

#include <iostream>

#pragma pack(1)
namespace tl_libfabric {
struct ConstVariables {
    const static uint64_t MINUS_ONE = -1;
    const static uint64_t ZERO = 0;

    const static uint32_t MAX_OVER_SCHEDULER_TS = 5;
    const static uint32_t MAX_HISTORY_SIZE = 200;
};
}
#pragma pack()
