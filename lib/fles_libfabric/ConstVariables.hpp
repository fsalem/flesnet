// Copyright 2017 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>


#pragma once

#include <iostream>

#pragma pack(1)
namespace tl_libfabric {
struct ConstVariables {
    const static uint64_t MINUS_ONE = -1;
    const static uint64_t ZERO = 0;
    const static uint32_t ONE_HUNDRED = 100;

    const static uint32_t SCHEDULER_INTERVAL_LENGTH = 50;
    const static uint32_t MAX_HISTORY_SIZE = 200;

    const static uint32_t SPEEDUP_FACTOR = 10;

};
}
#pragma pack()
