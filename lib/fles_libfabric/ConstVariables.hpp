// Copyright 2017 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>


#pragma once

#include <iostream>

#pragma pack(1)
namespace tl_libfabric {
struct ConstVariables {
    const static uint64_t MINUS_ONE = -1;
    const static uint64_t ZERO = 0;
    const static uint32_t ONE_HUNDRED = 100;

    const static uint32_t MAX_HISTORY_SIZE = 20000; // high for logging ... should be small ~100-200
    const static uint16_t MAX_TIMESLICE_PER_INTERVAL = 10000;

    ///-----
    const static bool ENABLE_LOGGING = 1;
    const static uint32_t MAX_MEDIAN_VALUES = 10;

    const static uint32_t SPEEDUP_SLOWDOWN_HISTORY = 10;

    static constexpr double SPEEDUP_ROUND_FACTOR = 0.0f;
    static constexpr double SPEEDUP_GAP_PERCENTAGE = 0.0;
    const static uint32_t SPEEDUP_INTERVAL_PERIOD = 10;

    static constexpr double SLOWDOWN_ROUND_FACTOR = 1.0f;
    static constexpr double SLOWDOWN_GAP_PERCENTAGE = 200.5;
    const static uint32_t SLOWDOWN_INTERVAL_PERIOD = 10;

    static std::string LOG_DIRECTORY;
    ///-----/
};
}
#pragma pack()
