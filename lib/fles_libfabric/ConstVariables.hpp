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

    static constexpr double SPEEDUP_FACTOR = 0.9975f;

    static constexpr double SLOWDOWN_FACTOR = 1.05f;

    const static bool ENABLE_LOGGING = 0;

    const static uint32_t MAX_MEDIAN_VALUES = 10;

    const static uint32_t SPEEDUP_SLOWDOWN_HISTORY = 10;

    static constexpr double SPEEDUP_GAP_PERCENTAGE = 0.25;

    static constexpr double SLOWDOWN_GAP_PERCENTAGE = 1.0;

    static std::string LOG_DIRECTORY;

};
}
#pragma pack()
